# Dahua P2P

Private P2P format from Dahua/KBVision cameras and NVRs, the same cloud
transport the DMSS mobile app uses. Streams RTSP from a device identified only
by its serial number, with no port forwarding, VPN or static IP.

## Configuration

- you can skip `channel` and `subtype` if they are default
- set up separate streams for different channels
- use `subtype=0` for Main stream, and `subtype=1` for Extra1 stream
- the device must be registered with the Dahua/easy4ip cloud and online
- credentials are the device's own username and password
- the host part of the URL is the device serial number, not an address

```yaml
streams:
  camera1: dahua://username:password@SERIAL?channel=1&subtype=0
  camera2: dahua://username:password@SERIAL?channel=2&subtype=1
```

Streams are discoverable from the `/add.html` page by serial and channel count.

## Tuning

- `max_realms` caps how many streams share one P2P tunnel; beyond the cap an
  additional tunnel is opened instead. Raising it is usually better than
  lowering it: a capture of the DMSS app carried 17 realms on one tunnel, and
  every extra tunnel costs a cloud handshake, which is the step that actually
  fails often.
- `negotiate_settle` is the interval held between RTSP negotiations against one
  device, paced device-wide rather than per tunnel. The same capture shows the
  device answering 12 realm binds sent inside 134ms, so this is not a limit the
  device imposes; it exists because this client interleaves a full RTSP
  negotiation with each bind. Raise it only if realm setup proves unreliable.
- `p2p_port` pins the local UDP port. P2P requires the device to reply to the
  same source port we punched from, so any NAT that rewrites it — notably
  Kubernetes pod networking — breaks the connection. Pin the port and expose it
  unchanged with `hostPort` or host networking. A pinned port carries one
  tunnel, so when that tunnel stops taking new streams it is torn down and
  rebuilt, and its existing streams reconnect onto the new one.

```yaml
dahua:
  max_realms: 8       # default
  negotiate_settle: 500ms  # default

streams:
  camera1: dahua://username:password@SERIAL?channel=1&p2p_port=51234
```

## Security

The P2P tunnel is not encrypted. Only the handshake is authenticated; the RTSP
exchange and the video that follow cross the internet in clear, guarded only by
a check on the sender's address. Anyone on the path can watch the stream.

Protocol details are in [`pkg/dahua`](../../pkg/dahua/README.md).
