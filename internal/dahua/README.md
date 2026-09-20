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

- `max_realms` caps how many streams share one P2P tunnel. Device firmware
  becomes unresponsive past some number of concurrent streams; beyond the cap
  an additional tunnel is opened instead. Lower it if streams stall under load.
- `p2p_port` pins the local UDP port. P2P requires the device to reply to the
  same source port we punched from, so any NAT that rewrites it — notably
  Kubernetes pod networking — breaks the connection. Pin the port and expose it
  unchanged with `hostPort` or host networking.

```yaml
dahua:
  max_realms: 8   # default

streams:
  camera1: dahua://username:password@SERIAL?channel=1&p2p_port=51234
```

Protocol details are in the standalone
[dahua-p2p library](https://github.com/blackjid/dahua-p2p).
