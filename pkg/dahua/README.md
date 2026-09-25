# Dahua P2P

Reverse-engineered implementation of the P2P transport Dahua/KBVision devices
use to reach the easy4ip cloud, carrying an ordinary RTSP session over a
NAT-punched UDP socket.

```
dh/      cloud protocol: UDP client, DH-HTTP, WSSE auth, device crypto, handshake
ptcp/    PTCP packet framing and session counters
tunnel/  TCP-over-PTCP realms, exposed as net.Conn
```

This package owns no logger. `Config.Trace` and `Config.Error` are wired by
`internal/dahua`; while nil, nothing is formatted.

## PTCP

PTCP ("phony TCP") multiplexes connections over one UDP socket as *realms*.

```
┌──────────────────── Header (24 bytes) ─────────────────────┐
│ "PTCP" │ Sent(4) │ Recv(4) │ PID(4) │ LMID(4) │ RMID(4)    │
├──────────────────── Body (variable) ───────────────────────┤
│ Type(1) │ Length(3) │ Realm(4) │ Padding(4) │ Data(N)      │
└────────────────────────────────────────────────────────────┘
```

`0x00` SYNC, `0x0a` NACK, `0x10` payload, `0x11` BIND, `0x12` status
(`CONN`/`DISC`), `0x13` heartbeat, `0x17`-`0x1B` handshake commands.

Neither ID field in the header is an identifier. Both were originally modelled
as counters, which is wrong in a way that degrades slowly, so a capture of the
DMSS app decides them:

- **LMID is a millisecond clock**, quantized to 10ms. Over 15.7s it advances at
  exactly 1.000 units per millisecond in both directions, and 1573 of 2610
  consecutive packets repeat the previous value. Packets sent inside one tick
  share an LMID, so nothing may treat it as unique or as a sequence number.
  `Stats.OutLagMillis` reads the device's echo of it as a round-trip time.
  What it carries is an *uptime*: 24 million for the app, 143 million for the
  device. Seeding it from the wall clock instead puts it above 2^31, which a
  peer storing it signed reads as negative, and the device then stops consuming
  anything we send. Ours is process uptime, masked to 31 bits.
- **PID is a receive-side byte count**, `0xFFFF` minus the bytes taken in since
  our previous transmission. It is not a dedup token: three BINDs for three
  different realms went out 10ms apart carrying `pid=63345` and the device
  granted all three, and the device used only 83 distinct values across 4776
  packets. Its value never strays more than ~4000 below `0xFFFF`. A decrementing
  packet counter leaves that band after a few thousand packets and wraps every
  65536, which is what froze the device's view of our receive window.

`sendPacket` still serializes the session-state read and the UDP write, so the
`Sent`/`Recv` a packet advertises are true of the moment it leaves.

Payloads fragment at 1280 bytes, matching the official app and keeping the
datagram (24 + 12 + 1280) inside common MTUs.

## Measuring loss

Inbound headers carry the device's view of the conversation, so loss is
observable without a capture. `Tunnel.Stats` exposes it and the tunnel traces
it every 30s, idle tunnels included:

```
ptcp counters sent=23314 peer_recv=23302 out_unacked=12 out_lag_ms=179
              recv=124174150 peer_sent=124172858 in_skew=-1292 missed_hb=0 realms=7
```

`out_unacked` returns to 0 in a healthy tunnel; sustained growth while realms
are writing means the device has stopped consuming what we send. That is the
failure that precedes a tunnel refusing every BIND, and it is invisible to
inbound liveness checks because the device keeps sending packets of its own
throughout. The heartbeat loop watches the same signal and rebuilds the tunnel
after `outboundStallTimeout`.

The device only reports its `Recv` in packets it sends, so over a tunnel
nobody is writing to there is no fresh number to read and `peer_recv` simply
stops moving. That is not a stall, and treating it as one tore down tunnels
that were serving live realms, so the check also requires that a realm has
written something since the counter last moved. A tunnel that has gone
entirely silent is caught by `missed_hb` instead, which allows six times as
long. `in_skew` is normally slightly *negative* — the device's snapshot
predates packets we already consumed — and sustained growth is inbound loss.
`out_lag_ms` is our clock minus the last reading of it the device echoed back.
It is not a round trip: the device advances `RMID` on the packets it answers
rather than on every ACK, so it rests at about the heartbeat interval. A
healthy sample, eight realms streaming 17 MB, read `out_unacked=0
out_lag_ms=5000 in_skew=-503`. It also climbs while the tunnel is idle,
because the clock runs whether or not we send.

## Reliability

`Sent` and `Recv` are byte offsets into each direction's stream, as in TCP, and
the device runs a reliable transport over them. Probed live:

- A data packet we never acknowledge is resent ~250ms later at the same `Sent`.
- A hole in *our* stream stops the device consuming anything after it. It asks
  for the missing bytes with a `0x0a` body, `0a 00 08 <offset> 00000000
  <length>`, repeated every ~10ms, and goes silent if they never come.
  Resending them resumes the stream at once.
- The `0x0a` NACK is out of band: the device's next data packet carries the
  same `Sent`, so it must not be counted.

So the session delivers inbound data in offset order, drops duplicates, holds
data that arrives past a hole, and skips a hole the device has not filled in
1.5s. The tunnel keeps every outbound packet until the device's `Recv` passes
it and resends the queue on a NACK, or when the device has answered since
without covering it. Before this, one lost datagram in either direction left
the two ends disagreeing about the stream for good: substreams rarely hit it,
eight main streams hit it within hours.
