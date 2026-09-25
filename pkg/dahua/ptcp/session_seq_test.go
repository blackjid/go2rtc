package ptcp

import "testing"

func data(sent uint32, n int) *Packet {
	return NewPacket(&Header{Sent: sent}, NewPayloadBody(1, make([]byte, n)))
}

func TestReceiveOrdersByOffset(t *testing.T) {
	s := NewSession()
	// 12-byte payload header, so each body is 12+n.
	if r, _ := s.Receive(data(0, 88)); len(r) != 1 {
		t.Fatalf("in-order packet not delivered: %d", len(r))
	}
	// 200 arrives before 100: held, not counted.
	if r, _ := s.Receive(data(200, 88)); len(r) != 0 || !s.Gap() {
		t.Fatalf("packet past a hole delivered early")
	}
	if got := s.Stats().Recv; got != 100 {
		t.Fatalf("recv advanced past a hole: %d", got)
	}
	// The hole fills: both come out, in order.
	r, _ := s.Receive(data(100, 88))
	if len(r) != 2 || r[0].Header.Sent != 100 || r[1].Header.Sent != 200 || s.Gap() {
		t.Fatalf("hole fill did not release in order: %v", r)
	}
	if got := s.Stats().Recv; got != 300 {
		t.Fatalf("recv = %d, want 300", got)
	}
	// A retransmission of something already taken is dropped, not counted.
	if r, dup := s.Receive(data(100, 88)); len(r) != 0 || !dup || s.Stats().Recv != 300 {
		t.Fatalf("duplicate was counted")
	}
}

func TestReceiveAckOnlyUpdatesPeer(t *testing.T) {
	s := NewSession()
	s.Receive(NewPacket(&Header{Sent: 0, Recv: 40}, NewEmptyBody()))
	s.Receive(NewPacket(&Header{Sent: 0, Recv: 20}, NewEmptyBody())) // reordered older ACK
	if got := s.Stats().PeerRecv; got != 40 {
		t.Fatalf("peer recv went backwards: %d", got)
	}
}

func TestSkipGap(t *testing.T) {
	s := NewSession()
	s.Receive(data(0, 88))
	s.Receive(data(300, 88))
	s.Receive(data(400, 88))
	r, skipped := s.SkipGap()
	if skipped != 200 || len(r) != 2 || s.Stats().Recv != 500 || s.Gap() {
		t.Fatalf("skip: skipped=%d ready=%d recv=%d", skipped, len(r), s.Stats().Recv)
	}
}

func TestResendKeepsOffset(t *testing.T) {
	s := NewSession()
	first := s.Send(NewPayloadBody(1, make([]byte, 88)))
	s.Send(NewHeartbeatBody())
	re := s.Resend(first.Header.Sent, first.Body)
	if re.Header.Sent != 0 || s.Stats().Sent != 112 {
		t.Fatalf("resend sent=%d, session sent=%d", re.Header.Sent, s.Stats().Sent)
	}
}

// The device's retransmit request is out of band: it must not move our
// position in its stream, or the data packet that follows at the same offset
// reads as a duplicate and is thrown away.
func TestNackIsNotCounted(t *testing.T) {
	s := NewSession()
	raw := append(NewPacket(&Header{Sent: 0}, NewEmptyBody()).Serialize(),
		0x0a, 0x00, 0x08, 0x53, 0, 0, 0, 0, 0x00, 0x3a, 0x00)
	nack, err := ParsePacket(raw)
	if err != nil || nack.Body.Type != BodyTypeNack {
		t.Fatalf("parse nack: %v %v", err, nack)
	}
	s.Receive(nack)
	if r, dup := s.Receive(data(0, 88)); len(r) != 1 || dup {
		t.Fatalf("data after a nack at the same offset was dropped")
	}
}
