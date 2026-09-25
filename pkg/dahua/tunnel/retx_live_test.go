//go:build live

package tunnel

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/dahua/dh"
	"github.com/AlexxIT/go2rtc/pkg/dahua/ptcp"
)

// TestLiveRetransmit probes a real device: does it resend a packet we never
// acknowledge, and does it keep consuming ours after we skip bytes?
//
//	DAHUA_SERIAL=.. DAHUA_USERNAME=.. DAHUA_PASSWORD=.. go test -tags live -run TestLiveRetransmit -v ./pkg/dahua/tunnel/
func TestLiveRetransmit(t *testing.T) {
	serial := os.Getenv("DAHUA_SERIAL")
	if serial == "" {
		t.Skip("DAHUA_SERIAL not set")
	}
	res, err := dh.Handshake(dh.HandshakeOptions{
		Serial: serial, DeviceUsername: os.Getenv("DAHUA_USERNAME"), DevicePassword: os.Getenv("DAHUA_PASSWORD"),
		Timeout: 10 * time.Second, Trace: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	c, s := res.Client, res.Session
	defer c.Close()
	start := time.Now()
	ms := func() int64 { return time.Since(start).Milliseconds() }

	send := func(b *ptcp.Body) *ptcp.Packet {
		p := s.Send(b)
		if err := c.Send(p.Serialize()); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ack := func() { send(ptcp.NewEmptyBody()) }

	// read returns the next packet, or nil after d.
	read := func(d time.Duration) (*ptcp.Packet, int) {
		raw, err := c.ReadRaw(d)
		if err != nil {
			return nil, 0
		}
		p, err := ptcp.ParsePacket(raw)
		if err != nil {
			t.Logf("%6dms unparseable len=%d", ms(), len(raw))
			return nil, 0
		}
		return p, len(raw) - ptcp.HeaderSize
	}
	logp := func(tag string, p *ptcp.Packet, blen int) {
		t.Logf("%6dms %s type=%d sent=%d recv=%d blen=%d ourRecv=%d ourSent=%d",
			ms(), tag, p.Body.Type, p.Header.Sent, p.Header.Recv, blen, s.Stats().Recv, s.Stats().Sent)
	}

	// 1. BIND a realm and wait for CONN.
	const realm = 0x0badf00d
	send(ptcp.NewBindBody(realm, res.RTSPPort))
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		p, n := read(500 * time.Millisecond)
		if p == nil {
			continue
		}
		s.Recv(p)
		logp("bind-phase", p, n)
		if p.Body.Len() > 0 {
			ack()
		}
		if p.Body.Type == ptcp.BodyTypeStatus && p.Body.Status == ptcp.StatusConnect {
			break
		}
	}

	// 2. Ask for OPTIONS and drop the reply: never count it, never ACK it.
	send(ptcp.NewPayloadBody(realm, []byte("OPTIONS rtsp://127.0.0.1/ RTSP/1.0\r\nCSeq: 1\r\n\r\n")))
	var dropped *ptcp.Packet
	hb := time.NewTicker(time.Second)
	defer hb.Stop()
	for deadline := time.Now().Add(12 * time.Second); time.Now().Before(deadline); {
		select {
		case <-hb.C:
			send(ptcp.NewEmptyBody()) // re-advertise our stale recv, like a dup-ack
		default:
		}
		p, n := read(200 * time.Millisecond)
		if p == nil {
			continue
		}
		if p.Body.Type == ptcp.BodyTypePayload && dropped == nil {
			dropped = p
			logp("DROPPED", p, n)
			continue
		}
		if dropped != nil && p.Body.Type == ptcp.BodyTypePayload && p.Header.Sent == dropped.Header.Sent {
			logp("RETRANSMIT", p, n)
			s.Recv(p)
			ack()
			break
		}
		logp("in", p, n)
		if dropped != nil && p.Header.Sent > dropped.Header.Sent && p.Body.Len() > 0 {
			t.Logf("         ^ device sent past the hole without retransmitting")
		}
		s.Recv(p)
		if p.Body.Len() > 0 {
			ack()
		}
	}

	// 3. Skip 100 of our own bytes, then send OPTIONS again: does the device
	// still consume it (peer Recv advances past the hole) or stall?
	t.Logf("%6dms --- outbound hole test ---", ms())
	holeBody := ptcp.NewPayloadBody(realm, []byte("OPTIONS rtsp://127.0.0.1/ RTSP/1.0\r\nCSeq: 2\r\n\r\n"))
	hole := s.Send(holeBody) // accounted, never sent
	send(ptcp.NewPayloadBody(realm, []byte("OPTIONS rtsp://127.0.0.1/ RTSP/1.0\r\nCSeq: 3\r\n\r\n")))
	resent := false
	for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline); {
		select {
		case <-hb.C:
			send(ptcp.NewHeartbeatBody())
		default:
		}
		p, n := read(200 * time.Millisecond)
		if p == nil {
			continue
		}
		logp("in", p, n)
		if p.Body.Type == ptcp.BodyTypeCommand {
			t.Logf("         command bytes=%x", p.Body.Command)
		}
		if p.Body.Type == ptcp.BodyTypePayload {
			t.Logf("         payload=%q", p.Body.Data[:min(40, len(p.Body.Data))])
		}
		s.Recv(p)
		if p.Body.Len() > 0 {
			ack()
		}
		if !resent && p.Body.Type == ptcp.BodyTypeCommand {
			resent = true
			re := s.Resend(hole.Header.Sent, holeBody)
			_ = c.Send(re.Serialize())
			t.Logf("%6dms RESENT hole at sent=%d", ms(), hole.Header.Sent)
		}
	}
	send(ptcp.NewStatusBody(realm, ptcp.StatusDisconnect))
}

// TestLiveTunnelRecoversOutboundLoss drives the production Tunnel: it loses
// one outbound RTSP request on purpose and expects the retransmitter to get
// the device past the hole, so a later request is still answered.
func TestLiveTunnelRecoversOutboundLoss(t *testing.T) {
	serial := os.Getenv("DAHUA_SERIAL")
	if serial == "" {
		t.Skip("DAHUA_SERIAL not set")
	}
	tn, err := New(Config{Serial: serial, Username: os.Getenv("DAHUA_USERNAME"), Password: os.Getenv("DAHUA_PASSWORD"),
		Trace: t.Logf, Error: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	defer tn.Close()
	conn, err := tn.Dial(tn.RTSPPort())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Lose one request: account for it and queue it, exactly as sendPacket
	// would, but never put it on the wire.
	if err := tn.acquireSendPermit(nil); err != nil {
		t.Fatal(err)
	}
	lost := ptcp.NewPayloadBody(conn.realmID, []byte("OPTIONS rtsp://127.0.0.1/ RTSP/1.0\r\nCSeq: 1\r\n\r\n"))
	p := tn.session.Send(lost)
	tn.outMu.Lock()
	tn.outq = append(tn.outq, outPacket{seq: p.Header.Sent, body: lost, at: time.Now()})
	tn.outMu.Unlock()
	tn.sendPermit <- struct{}{}

	start := time.Now()
	if _, err := conn.Write([]byte("OPTIONS rtsp://127.0.0.1/ RTSP/1.0\r\nCSeq: 2\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 4096)
	var got string
	for !strings.Contains(got, "CSeq: 2") {
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("no answer past the hole after %s (read %q): %v", time.Since(start), got, err)
		}
		got += string(buf[:n])
	}
	st := tn.Stats()
	t.Logf("recovered in %s; both replies=%v out_unacked=%d", time.Since(start),
		strings.Contains(got, "CSeq: 1"), st.OutBytes())
}
