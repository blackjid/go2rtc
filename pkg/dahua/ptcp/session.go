package ptcp

import (
	"sync"
	"time"
)

// Session manages the PTCP session state
type Session struct {
	mu   sync.Mutex
	sent uint32 // Total bytes sent
	recv uint32 // Total bytes received
	rmid uint32 // Remote message ID (from last received packet)

	// lastPIDRecv is s.recv as of our previous transmission. PID reports the
	// bytes taken in since then; see pid.
	lastPIDRecv uint32

	// clock supplies LMID. Both endpoints put a millisecond clock there, not
	// a counter; see lmid.
	clock  func() uint32
	epoch  time.Time
	origin uint32

	// The peer's view of the same conversation, taken from the last packet
	// it sent us. peerRecv/peerRMID are its acknowledgement of our data;
	// peerSent is how much it believes it has sent us.
	peerSent uint32
	peerRecv uint32
	peerRMID uint32

	// pending holds data that arrived past a hole, keyed by the byte offset
	// the peer stamped on it, until the hole is filled or skipped.
	pending map[uint32]*Packet
}

// maxPending bounds how many out-of-order packets are held for a hole.
const maxPending = 1024

// Stats is a snapshot of both endpoints' counters, used to detect loss.
type Stats struct {
	Sent uint32 // body bytes we have sent
	Recv uint32 // body bytes we have counted as received
	LMID uint32 // our millisecond clock as of the last packet sent

	PeerSent uint32 // body bytes the device says it has sent
	PeerRecv uint32 // body bytes the device says it has received from us
	PeerRMID uint32 // our clock as of the last packet the device acknowledged
}

// delta subtracts two wrapping uint32 counters as a signed value, so a
// counter that has run slightly ahead reads as a small negative number
// rather than ~4 billion.
func delta(a, b uint32) int64 { return int64(int32(a - b)) }

// OutBytes is our data the device has not acknowledged. It is normally a
// small in-flight amount that keeps returning to 0; a value that only grows
// means outbound packets are being lost.
func (t Stats) OutBytes() int64 { return delta(t.Sent, t.PeerRecv) }

// OutLagMillis is how far our clock has run on past the last reading of it
// the device echoed back. It is not a round trip: the device advances RMID on
// the packets it answers rather than on every ACK, so on a healthy tunnel the
// figure sits at roughly our heartbeat interval — 5000 measured against a
// device streaming 17MB across eight realms with OutBytes at 0. Read it for
// growth past that floor, and only against the moment a packet was last sent,
// since the clock advances whether or not we send anything.
func (t Stats) OutLagMillis() int64 { return delta(t.LMID, t.PeerRMID) }

// InBytes is data the device claims to have sent that we never counted.
// Slightly negative is normal and means the opposite: the device's Sent in
// the header we are reading predates packets we have already consumed, so we
// are up to one packet ahead of its snapshot. Only sustained growth is loss.
func (t Stats) InBytes() int64 { return delta(t.PeerSent, t.Recv) }

// Stats returns a snapshot of both endpoints' counters.
func (s *Session) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		Sent: s.sent, Recv: s.recv, LMID: s.lmid(),
		PeerSent: s.peerSent, PeerRecv: s.peerRecv, PeerRMID: s.peerRMID,
	}
}

// processStart anchors the LMID clock. Both endpoints carry an uptime in that
// field — 24 million for the app in the capture, 143 million for the device —
// so the clock belongs to the process, not to one session, and a tunnel
// rebuilt an hour in carries on from where the last one left off.
var processStart = time.Now()

// lmidBase keeps the clock clear of 0, which is the value RMID carries before
// anything has been received.
const lmidBase = 60_000

// lmidMask holds the clock below 2^31, where a peer storing it signed would
// read it as negative. Seeding it from the wall clock put it there outright;
// plain uptime gets there too, after 24.8 days. Neither endpoint in the
// capture emits anything near that — the app carried 24 million and the device
// 143 million — so the clock wraps at 24.8 days of uptime rather than ever
// leaving the band they use. RMID deltas are computed as wrapping signed
// values, so the wrap costs one stale reading, not a stuck one.
const lmidMask = 0x7FFFFFFF

// NewSession creates a new PTCP session
func NewSession() *Session {
	return newSessionAt(processStart)
}

func newSessionAt(start time.Time) *Session {
	s := &Session{epoch: start, origin: lmidBase}
	s.clock = func() uint32 {
		ms := uint32(time.Since(s.epoch).Milliseconds() & lmidMask)
		return (s.origin + quantizeLMID(ms)) & lmidMask
	}
	return s
}

// lmidTick is the granularity the endpoints quantize their clock to. Every
// LMID in a capture is a multiple of it (offset by a per-endpoint constant),
// so packets sent inside the same tick share one.
const lmidTick = 10

func quantizeLMID(ms uint32) uint32 { return ms - ms%lmidTick }

// lmid is the millisecond clock both endpoints carry in the LMID field.
// Measured against a DMSS capture it advances at exactly 1.000 units per
// millisecond in both directions across 15.7s, and 1573 of 2610 consecutive
// packets repeat the previous value. It is a timestamp, not a sequence
// number: nothing downstream may assume it is unique per packet or that it
// counts anything.
//
// Must be called with s.mu held.
func (s *Session) lmid() uint32 { return s.clock() }

// pid fills the PID field, which the DMSS capture shows is not a packet
// identifier: three BINDs for three different realms went out 10ms apart
// carrying pid=63345 and the device granted all three, and the device itself
// used only 83 distinct values across 4776 packets. The low 16 bits hold
// 0xFFFF minus a small byte count derived from what has been received, never
// straying more than ~4000 below 0xFFFF.
//
// We report the bytes taken in since our previous transmission, which matches
// the capture on about half its packets and, more to the point, keeps the
// field in the band the device produces. The old monotonic packet counter
// left that band after ~4000 packets and wrapped every ~65536, sweeping a
// field the device reads as a receive-side quantity.
//
// Must be called with s.mu held.
func (s *Session) pid() uint32 {
	taken := s.recv - s.lastPIDRecv
	if taken > 0xFFFF {
		taken = 0xFFFF
	}
	return 0x0000FFFF - taken
}

// Send creates a packet for the given body and updates session state
func (s *Session) Send(body *Body) *Packet {
	s.mu.Lock()
	defer s.mu.Unlock()

	header := &Header{
		Sent: s.sent,
		Recv: s.recv,
		LMID: s.lmid(),
		RMID: s.rmid,
	}

	if body.Type == BodyTypeSync {
		header.PID = 0x0002FFFF
	} else {
		header.PID = s.pid()
		s.lastPIDRecv = s.recv
	}

	s.sent += uint32(body.Len())

	return NewPacket(header, body)
}

// Recv files a received packet; see Receive. It is for callers that handle
// one packet at a time in lockstep, like the handshake.
func (s *Session) Recv(packet *Packet) *Packet {
	s.Receive(packet)
	return packet
}

// Receive files an inbound packet and returns the packets that are now
// deliverable, in byte order.
//
// Sent and Recv in the header are byte offsets into each direction's stream,
// as in TCP, not free-running counters: a packet's Sent is the offset of its
// first body byte. Counting every arrival as the next bytes -- what this used
// to do -- turns one lost datagram into a permanent disagreement about where
// the stream is, and the device stops consuming anything past it. So a
// packet at our offset is delivered, one behind it is a duplicate and
// dropped, and one ahead of it waits in pending for the hole to fill.
func (s *Session) Receive(packet *Packet) (ready []*Packet, dup bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	h := packet.Header
	s.rmid = h.LMID
	if delta(h.Sent, s.peerSent) > 0 || s.peerSent == 0 {
		s.peerSent = h.Sent
	}
	if delta(h.Recv, s.peerRecv) > 0 || s.peerRecv == 0 {
		s.peerRecv = h.Recv
	}
	s.peerRMID = h.RMID

	n := uint32(packet.Body.Len())
	if n == 0 {
		return nil, false
	}
	switch off := delta(h.Sent, s.recv); {
	case off < 0:
		return nil, true
	case off > 0:
		if s.pending == nil {
			s.pending = make(map[uint32]*Packet)
		}
		if len(s.pending) < maxPending {
			s.pending[h.Sent] = packet
		}
		return nil, false
	}
	s.recv += n
	return s.drain(append(ready, packet)), false
}

// drain appends every pending packet that the stream has now reached.
// Must be called with s.mu held.
func (s *Session) drain(ready []*Packet) []*Packet {
	for {
		p, ok := s.pending[s.recv]
		if !ok {
			break
		}
		delete(s.pending, s.recv)
		s.recv += uint32(p.Body.Len())
		ready = append(ready, p)
	}
	// Anything the stream has overtaken is stale.
	for off := range s.pending {
		if delta(off, s.recv) < 0 {
			delete(s.pending, off)
		}
	}
	return ready
}

// Gap reports whether data is waiting behind a hole in the inbound stream.
func (s *Session) Gap() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending) > 0
}

// SkipGap gives up on the hole: the stream jumps to the earliest data that
// did arrive, and that data and whatever follows it is returned. skipped is
// how many bytes were never received.
func (s *Session) SkipGap() (ready []*Packet, skipped uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	first, ok := uint32(0), false
	for off := range s.pending {
		if !ok || delta(off, first) < 0 {
			first, ok = off, true
		}
	}
	if !ok {
		return nil, 0
	}
	skipped = first - s.recv
	s.recv = first
	return s.drain(nil), skipped
}

// Resend rebuilds a packet already sent at offset seq, with the current
// acknowledgement fields, without advancing our own offset.
func (s *Session) Resend(seq uint32, body *Body) *Packet {
	s.mu.Lock()
	defer s.mu.Unlock()
	header := &Header{Sent: seq, Recv: s.recv, LMID: s.lmid(), RMID: s.rmid, PID: s.pid()}
	s.lastPIDRecv = s.recv
	return NewPacket(header, body)
}
