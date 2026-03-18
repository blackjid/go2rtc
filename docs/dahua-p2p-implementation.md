# Dahua P2P Protocol Implementation for go2rtc

## Overview

This document describes the implementation of the Dahua P2P protocol in go2rtc, enabling remote RTSP streaming from Dahua/KBVision NVRs and cameras without port forwarding or VPN. The device is identified only by its serial number; all NAT traversal, authentication, and tunneling happen automatically through Dahua's cloud infrastructure.

The implementation lives in `pkg/dahua/` (protocol layer) and `internal/dahua/` (go2rtc integration) and introduces a new `dahua://` URL scheme:

```yaml
streams:
  camera.entrance:
    - dahua://user:pass@SERIAL_NUMBER?channel=3&subtype=0
```

---

## Architecture

### Package Structure

```
pkg/dahua/
├── dh/
│   ├── protocol.go    # UDP client, DH-HTTP request/response, WSSE auth
│   ├── handshake.go   # 12-step P2P handshake with NAT traversal
│   └── crypto.go      # AES device info decryption, PBKDF2, HMAC auth
├── ptcp/
│   ├── types.go       # PTCP packet types (SYNC, BIND, CONN, PAYLOAD, etc.)
│   └── session.go     # PTCP session state (PID, counters, flow control)
├── tunnel/
│   └── tunnel.go      # TCP-over-PTCP tunnel with heartbeat, realms, Listener
└── dahua.go           # Client, SessionManager (refcounting, idle timeout)

internal/dahua/
└── dahua.go           # go2rtc integration: dahua:// handler, retry logic
```

### Data Flow

```
Browser (WebRTC) ──► go2rtc ──► RTSP Client ──► TCP Listener (127.0.0.1:random)
                                                      │
                                               ┌──────┴──────┐
                                               │  Tunnel.Conn │  (implements net.Conn)
                                               │  realm=N     │
                                               └──────┬──────┘
                                                      │ PTCP Payload packets
                                                      │ (UDP, 1280-byte chunks)
                                               ┌──────┴──────┐
                                               │  UDP Socket  │
                                               │  (P2P hole)  │
                                               └──────┬──────┘
                                                      │ Internet
                                                      ▼
                                               ┌─────────────┐
                                               │ Dahua Device │
                                               │ RTSP :554    │
                                               └─────────────┘
```

Each camera channel gets its own **realm** (a multiplexed virtual connection within a single UDP tunnel). A realm is created by sending a BIND request for port 554 and waiting for the device's CONN response. Data then flows as PTCP Payload packets tagged with that realm ID.

---

## The P2P Handshake (12 Steps)

The handshake is the most complex part of the protocol. It orchestrates communication between four parties: the client, Dahua's main server, a P2P relay agent, and the camera/NVR itself. The entire handshake is implemented in `dh/handshake.go`.

### Step-by-step

| Step | Action | Server | Purpose |
|------|--------|--------|---------|
| 1 | `DHGET /probe/p2psrv` | `easy4ipcloud.com:8800` | Discover P2P infrastructure |
| 2 | `DHGET /online/p2psrv/{serial}` | Main server | Get P2P server for this device |
| 3 | `DHGET /probe/device/{serial}` | P2P server | Verify device is online |
| 4 | `DHGET /info/device/{serial}` | P2P server | Get encrypted device info (randsalt, RTSP port) |
| 5 | `DHPOST /device/{serial}/p2p-channel` | Main server | Request P2P channel (with AES-encrypted local address) |
| 6 | `DHGET /relay/agent` | Relay server | Get agent address and token |
| 7 | `DHGET /relay/start/{token}` | Agent | Start relay session |
| 8 | Read P2P channel response | Main server | Get device's public and local addresses |
| 9 | `DHGET /device/{serial}/relay-channel` | Main server | Request relay channel for agent |
| 10 | PTCP SYNC with agent | Agent | Establish PTCP session with relay |
| 11 | Command 0x17 → agent | Agent | Get cryptographic **sign** for device authentication |
| 12 | STUN + PTCP SYNC + Command 0x19 → device | Device (direct) | NAT hole-punch, authenticate with sign |

### Device Authentication (Steps 4-5, 11-12)

Dahua devices with firmware that returns a `randsalt` require per-device authentication. The flow is:

1. **Derive device key**: `MD5("username:Login to randsalt:password")` as uppercase hex (32 bytes)
2. **Encrypt local address**: PBKDF2-SHA256 (20,000 iterations) + AES-OFB with a fixed IV
3. **Sign exchange**: The agent provides a cryptographic sign (command 0x17) that the client forwards to the device (command 0x19) to prove it went through the legitimate P2P infrastructure
4. **Device response**: Command 0x1A confirms authentication; client acknowledges with 0x1B

### NAT Traversal (Step 12)

The direct connection uses a STUN-like handshake:

1. Client sends a specially crafted packet to the device's **public** address (from step 8) containing an inverted Client ID (CID) and the device's address encoded in a STUN-like format
2. Client simultaneously sends the same packet to the device's **local/LAN** address (for same-network cameras)
3. Whichever address responds first wins — the UDP socket is reconnected to that address
4. A confirmation packet (magic `0xFEFEFFF3`) completes the STUN exchange
5. The PTCP session begins with SYNC, followed by sign-based authentication

```go
// STUN request structure
data := []byte{0xff, 0xfe, 0xff, 0xe7}  // magic
data = append(data, cookie...)            // 4 random bytes
data = append(data, transID...)           // 12 random bytes
data = append(data, 0x7f, 0xd5, 0xff, 0xf7)
data = append(data, invertedCID...)       // ~CID
data = append(data, 0xff, 0xfb, 0xff, 0xf7, 0xff, 0xfe)
data = append(data, ipToBytes(pubAddr)...)  // inverted IP:port
```

---

## The PTCP Protocol

PTCP ("PhonyTCP") is Dahua's custom protocol that multiplexes TCP connections over a single UDP socket. It provides reliable, ordered delivery with flow control — essentially TCP semantics over UDP for NAT traversal.

### Packet Structure

```
┌────────────────────── Header (24 bytes) ──────────────────────┐
│ "PTCP" (4) │ Sent (4) │ Recv (4) │ PID (4) │ LMID (4) │ RMID (4) │
├────────────────────── Body (variable) ──────────────────────────┤
│ Type (1) │ Length (3) │ Realm (4) │ Padding (4) │ Data (N)   │
└───────────────────────────────────────────────────────────────────┘
```

### Packet Types

| Type | Byte | Purpose |
|------|------|---------|
| SYNC | `0x00` | Session initialization (body: `0x00030100`) |
| Payload | `0x10` | TCP data, tagged with realm ID |
| BIND | `0x11` | Request a new realm → port mapping |
| Status | `0x12` | CONN (realm ready) or DISC (realm closed) |
| Heartbeat | `0x13` | Keep-alive, empty body |
| Command | `0x17-0x1B` | Authentication and sign exchange |

### Session State (PID Bug Fix)

The `ptcp.Session` tracks sent/received byte counts, a packet counter, and message IDs. One critical discovery during development:

**Every non-SYNC packet must increment the packet counter** to get a unique PID. Originally, empty ACK packets and heartbeats shared the same counter value, producing duplicate PIDs. The device uses PID for deduplication and silently discards duplicates — causing ACKs to be lost and the stream to stall after a few frames.

```go
// Fixed: all non-sync packets increment count
if body.Type != BodyTypeSync {
    s.count++
}
```

Before this fix, video would play for 2-3 seconds, then freeze. The device kept sending data, but the client's ACKs were being discarded as duplicate PIDs, so the device eventually stopped transmitting.

---

## Tunnel Architecture

### Realms (Multiplexed Connections)

A single P2P tunnel supports multiple concurrent connections through **realms**. Each realm is a virtual TCP connection to a port on the device (typically 554 for RTSP):

```
Tunnel (1 UDP socket)
├── Realm 0 → RTSP session for channel 3
├── Realm 1 → RTSP session for channel 5
├── Realm 2 → RTSP session for channel 8
└── ... up to device limit
```

Creating a realm:
1. Send BIND packet with realm ID and target port (554)
2. Wait for Status/CONN response from device
3. If no CONN within 5 seconds, retry up to 3 times (UDP packets can be lost)
4. Once CONN is received, data flows as Payload packets tagged with that realm ID

### The `net.Conn` Bridge

`tunnel.Conn` implements Go's `net.Conn` interface, allowing standard `io.Copy` to bridge between a local TCP socket and the PTCP tunnel:

```go
// TCP connection arrives at local listener
func (l *Listener) handleConn(tcpConn net.Conn) {
    tunnelConn, _ := l.tunnel.Dial(l.port)  // creates realm, sends BIND

    go io.Copy(tunnelConn, tcpConn)  // TCP → PTCP (fragmented into 1280-byte chunks)
    go io.Copy(tcpConn, tunnelConn)  // PTCP → TCP (reassembled from channel)
}
```

This design means RTSP negotiation happens over standard TCP — the RTSP client connects to `127.0.0.1:{random_port}` and is completely unaware it's going through a P2P tunnel.

### Write Fragmentation

RTSP responses can be large (DESCRIBE response, video keyframes). UDP has a practical MTU limit, so writes are fragmented:

```go
const maxPayloadSize = 1280  // matches DMSS app behavior

func (c *Conn) Write(p []byte) (n int, err error) {
    for len(p) > 0 {
        chunk := p
        if len(chunk) > maxPayloadSize {
            chunk = p[:maxPayloadSize]
        }
        // Send as PTCP Payload with realm ID
        packet := c.tunnel.session.Send(ptcp.NewPayloadBody(c.realmID, chunk))
        c.tunnel.client.Send(packet.Serialize())
        ...
    }
}
```

### Heartbeat and Health Monitoring

The tunnel sends heartbeat packets every 5 seconds. The `missedHeartbeats` counter increments with each sent heartbeat and resets to zero whenever **any** packet is received from the device (not just heartbeat responses). This is critical — video data packets also serve as implicit heartbeat acknowledgments.

If the counter exceeds `maxMissedHeartbeats` (currently 24, i.e., 2 minutes), the tunnel is closed. This generous window exists because the device can temporarily stop responding during heavy RTSP negotiation bursts.

---

## Session Management

### The Problem

A P2P handshake takes 3-5 seconds and involves 12 network round-trips through multiple servers. If every camera channel triggered its own handshake, a device with 7 channels would need 7 separate tunnels — 35+ seconds of setup and 7 times the UDP traffic.

### The Solution: `SessionManager`

The `SessionManager` provides tunnel sharing with reference counting:

```
SessionManager
├── sessions["9H0D892PAZ43131"]
│   ├── client: *Client (shared tunnel)
│   ├── refCount: 3 (3 active streams)
│   └── idleTimer: nil (active, no timer)
└── inflight["ABC123"]
    └── done: chan (handshake in progress, others wait)
```

**Acquire/Release lifecycle**:

1. `Acquire(serial)` — returns cached client if available, or creates new tunnel
2. Multiple goroutines calling `Acquire` for the same serial → first one performs the handshake, others wait on `inflightConn.done` channel
3. `Release(serial)` — decrements refcount; when zero, starts a 5-minute idle timer
4. If a new `Acquire` arrives within the idle window → cancels timer, reuses tunnel
5. `Invalidate(serial)` — force-closes tunnel and applies a 30-second cooldown (used when tunnel is confirmed broken)

### Idle Timeout and Deadlock Fix

The idle timer fires after 5 minutes of zero references. During development, this caused a **deadlock**:

```
Timer callback acquires m.mu
  → calls s2.client.Close()
    → triggers OnClose callback
      → OnClose tries to acquire m.mu
        → DEADLOCK
```

**Fix**: The timer callback collects the session to close while holding the lock, then calls `Close()` after releasing it:

```go
s.idleTimer = time.AfterFunc(m.IdleTimeout, func() {
    m.mu.Lock()
    s2, ok := m.sessions[serial]
    if !ok || s2 != s || s2.refCount > 0 {
        m.mu.Unlock()
        return
    }
    delete(m.sessions, serial)
    m.mu.Unlock()

    s2.client.Close()  // outside the lock
})
```

---

## RTSP Negotiation Serialization

### Discovery

When multiple camera channels connect simultaneously through the same tunnel, the device's RTSP server cannot reliably handle concurrent negotiations. Symptoms observed:

- Channel A's DESCRIBE succeeds; channel B's DESCRIBE times out
- BINDs for channels C and D never receive CONN
- The device stops responding to heartbeats entirely

### Solution: `NegotiateMu` + `WaitSettle`

Two mechanisms work together:

1. **`NegotiateMu`** (`sync.Mutex` on `Client`): serializes the entire Listen → BIND → Dial → DESCRIBE sequence, ensuring only one channel negotiates at a time

2. **`WaitSettle`**: enforces a minimum 5-second gap between consecutive negotiations, giving the device time to stabilize after setting up each RTSP session

```go
client.NegotiateMu.Lock()
client.WaitSettle()  // sleep if <5s since last negotiation

listener, _ := client.Listen("127.0.0.1:0", 554)
conn := rtsp.NewClient(rtspURL)
conn.Dial()
conn.Describe()

client.DoneNegotiate()  // record completion time
client.NegotiateMu.Unlock()
```

With 7 channels, this means ~35 seconds of sequential setup. This is acceptable because the `preload` feature (see below) runs this at startup in the background.

### Graceful Error Handling (The "No-Nuke" Fix)

An early implementation called `sessions.Invalidate(serial)` whenever any RTSP negotiation failed. This was catastrophic: one channel's timeout would close the entire shared tunnel, killing all 6 other active streams.

**Fix**: Only invalidate when the tunnel itself is confirmed dead:

```go
if err := conn.Describe(); err != nil {
    if client.IsClosed() {
        sessions.Invalidate(serial)  // tunnel is dead, rebuild
    } else {
        sessions.Release(serial)     // tunnel alive, just release ref
    }
    return nil, fmt.Errorf("RTSP describe failed: %w", err)
}
```

---

## Deployment: Kubernetes Challenges

### Kubernetes NAT vs P2P

Running P2P protocols inside Kubernetes presents unique challenges:

- **Pod network**: Each pod has its own network namespace with NAT
- **STUN responses**: The device sends its STUN response to the pod's public IP, but iptables rules can interfere with routing
- **`hostPort`**: We initially tried binding a fixed UDP port (`p2p_port=19000`) with `hostPort` to bypass pod NAT. This caused STUN timeouts because the `hostPort` iptables rules interfered with the UDP hole-punching

**Resolution**: Using a random ephemeral port (the default) works reliably. Kubernetes CNI handles the UDP NAT correctly for outbound connections; the issue was specifically with `hostPort`'s inbound iptables rules conflicting with the bidirectional UDP hole.

### Docker Multi-Architecture Builds

The development machine runs macOS/arm64 while the cluster runs linux/amd64. Images must be built with `--platform linux/amd64`:

```bash
docker build --platform linux/amd64 -t ghcr.io/user/go2rtc:sha -f docker/Dockerfile .
```

Missing this produces a cryptic `ErrImagePull` with "no match for platform in manifest".

### Image Caching (Spegel)

The cluster runs Spegel, a peer-to-peer image registry. Using `:latest` tags caused stale image issues — the cluster served cached copies instead of pulling the new image. **Fix**: Tag every build with the git SHA.

---

## Stream Lifecycle and the `preload` Solution

### The Refresh Storm

By default, go2rtc tears down the RTSP producer when the last WebRTC consumer disconnects. On a browser refresh:

1. All WebRTC consumers disconnect simultaneously
2. `RemoveConsumer` → `stopProducers()` → RTSP connections close
3. `sessions.Release(serial)` for each; tunnel stays alive (idle timeout)
4. Milliseconds later, browser reconnects → all channels re-negotiate simultaneously
5. A burst of BIND/DESCRIBE requests overwhelms the device

This manifests as: first load works, refresh shows errors, sometimes some streams recover, sometimes none do.

### Preload: Keep Streams Alive

go2rtc's `preload` feature creates a permanent internal consumer that keeps producers alive even with zero browser viewers:

```yaml
preload:
  camera.entrance:
  camera.entrance_2:
  camera.porton_inside:
```

With preload:
- Startup: go2rtc connects all streams (sequentially, with settle delays)
- Browser connects: attaches to already-running producers → instant video
- Browser refreshes: preload consumer still holds tracks → producers stay alive
- **Zero P2P churn on refresh**

The tradeoff is constant bandwidth (streams always flowing), which is acceptable for security cameras.

---

## Troubleshooting Log: Problems and Fixes

### 1. Stream Freezes After 2-3 Seconds

**Symptom**: Video plays briefly, then freezes. Device keeps sending data. No errors in log.

**Root cause**: Duplicate PIDs in PTCP packets. Empty ACK and heartbeat packets were not incrementing `s.count`, so they shared PIDs with data packets. The device deduplicated by PID and discarded the ACKs, eventually flow-controlling the stream.

**Fix**: Increment `s.count` for all non-SYNC packets in `ptcp/session.go`.

### 2. Multi-Stream Failures (BIND Storms)

**Symptom**: First stream connects; second and third time out on BIND or DESCRIBE.

**Root cause**: Concurrent BIND requests confuse the device. It can only process one BIND → CONN handshake at a time.

**Fix**: Added `dialMu` mutex to `Tunnel` to serialize `Dial` calls (and thus BINDs).

### 3. RTSP DESCRIBE Timeouts Under Load

**Symptom**: BIND → CONN succeeds, but DESCRIBE times out 5 seconds later.

**Root cause**: The device's RTSP server cannot handle concurrent OPTIONS/DESCRIBE/SETUP through different PTCP realms.

**Fix**: Added `NegotiateMu` mutex to serialize the entire Listen → Dial → Describe sequence per device.

### 4. Heartbeat Timeout During Setup

**Symptom**: `too many missed heartbeats, closing tunnel` — kills all active streams.

**Root cause**: While setting up multiple RTSP sessions (BIND/DESCRIBE cycles), the device becomes too busy to respond to heartbeats. With `maxMissedHeartbeats=3` (15 seconds), setup of 4+ channels reliably triggers this.

**Fix progression**:
- `3 → 6` (30 seconds): helped but not enough for 7 channels
- `6 → 24` (2 minutes): generous enough for full setup; RTSP read timeout (15s) provides faster dead-tunnel detection

### 5. One Failed Stream Kills All Streams

**Symptom**: RTSP DESCRIBE timeout on channel 5 causes channels 3, 8, and 10 to all disconnect.

**Root cause**: `sessions.Invalidate(serial)` was called on DESCRIBE failure, which force-closes the entire shared tunnel.

**Fix**: Only call `Invalidate` when the client is already closed (tunnel confirmed dead). Otherwise, call `Release` to just decrement the refcount.

### 6. Listener Close Leaves Zombie Realms

**Symptom**: After browser disconnect, the device reports connections on realms 0-3 but new streams can't allocate those realm IDs.

**Root cause**: `Listener.Close()` only stopped accepting new TCP connections. Existing `handleConn` goroutines (and their tunnel connections) were left running, occupying realms on the device.

**Fix**: `Listener` tracks all active `Conn` instances in a map and explicitly closes them when `Listener.Close()` is called.

### 7. SessionManager Deadlock

**Symptom**: After 5 minutes of inactivity, the next stream connection hangs forever. No log output.

**Root cause**: The idle timer callback acquired `m.mu`, then called `s.client.Close()`, which triggered `OnClose` — which also tried to acquire `m.mu`. Classic mutex deadlock.

**Fix**: Release the mutex before calling `Close()`. The `OnClose` callback checks if the session still exists in the map (it was already deleted).

### 8. Handshake Cooldown Cascade

**Symptom**: One P2P handshake fails → all 7 channels fail for 30 seconds with "connection cooldown active".

**Root cause**: `SessionManager.Acquire` set `m.lastFailed[serial] = time.Now()` on handshake failure, blocking all subsequent attempts for the same serial number. With a flaky relay infrastructure, a single transient failure cascaded to a full blackout.

**Fix**: Removed cooldown application from the handshake failure path. Cooldown is now only applied via `Invalidate()` (confirmed broken tunnel). The per-stream retry backoff (35 seconds) provides sufficient spacing.

### 9. `hostPort` Breaks STUN in Kubernetes

**Symptom**: P2P handshake completes through relay infrastructure, but the direct connection (step 12) always times out with "timeout waiting for device response".

**Root cause**: `hostPort` in Kubernetes adds iptables DNAT/SNAT rules for the specified port. These rules interfere with the bidirectional UDP hole-punching that STUN relies on — the STUN response from the device is mangled or dropped by the NAT rules.

**Fix**: Removed `p2p_port` configuration; using random ephemeral ports. Kubernetes CNI handles outbound UDP NAT correctly without `hostPort`.

---

## Current Limitations and Open Issues

### Device Concurrency Ceiling

The Dahua NVR tested (serial `9H0D892PAZ43131`) can reliably handle **3-5 simultaneous PTCP realms** through a single P2P tunnel. Beyond that, the device becomes overwhelmed:

- Stops responding to heartbeats
- Stops sending CONN responses to new BIND requests
- Existing video streams may stall

This is a firmware limitation, not a bug in the implementation. Mitigation strategies:
- Use `preload` for the most critical streams only (3-4)
- Remaining streams connect on-demand when the dashboard opens
- Sequential setup with settle delays prevents the burst that triggers overload

### RTSP Command Timeout

The RTSP library uses a package-level `Timeout` of 5 seconds for DESCRIBE/SETUP commands. This is separate from the per-connection `conn.Timeout` (set to 15 seconds for frame reads). Under heavy load, the device may take longer than 5 seconds to respond to DESCRIBE through the P2P tunnel. This is currently not configurable per-connection.

### Relay Mode

Relay mode (routing traffic through Dahua's relay servers instead of direct P2P) was implemented and then removed. The relay infrastructure proved unreliable:
- Sign exchange (command 0x17) frequently times out
- Relay servers are geographically distant, adding latency
- The relay protocol requires additional command exchanges (0x17/0x19) that are flaky

Direct P2P is significantly more reliable and lower latency. The relay codepath was removed to reduce complexity.

---

## Commit History

| SHA | Description |
|-----|-------------|
| `af4469c` | Initial P2P protocol implementation with PTCP tunneling |
| `e35d592` | Fix PTCP flow control: unique PIDs for ACK packets |
| `e104c1c` | Serialize BIND requests to prevent device overload |
| `3f99b21` | Handle SYNC packets in PTCP reader |
| `d3553b4` | Remove relay mode, focus on direct P2P |
| `34c1265` | Close tunnel connections when listener closes |
| `7f67a80` | Fix deadlock in session idle timer callback |
| `38cc2b6` | Improve P2P tunnel resilience: NegotiateMu, no-nuke errors, heartbeat tolerance |
| `bc533a0` | Remove cooldown on handshake failures |

---

## References

- **DMSS app**: Dahua's official mobile app, used as reference for PTCP packet sizes (1280-byte payloads) and protocol behavior
- **Wireshark captures**: `dahua_capture.pcap` and `dahua_capture2.pcap` were used during development to reverse-engineer the PTCP framing and handshake sequence
- **easy4ipcloud.com**: Dahua's P2P cloud service (main server, `port 8800`)
