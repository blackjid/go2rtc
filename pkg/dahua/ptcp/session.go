package ptcp

import (
	"sync"
)

// Session manages the PTCP session state
type Session struct {
	mu    sync.Mutex
	sent  uint32 // Total bytes sent
	recv  uint32 // Total bytes received
	count uint32 // Packet counter (excluding sync/empty)
	id    uint32 // Local message ID counter
	rmid  uint32 // Remote message ID (from last received packet)
}

// NewSession creates a new PTCP session
func NewSession() *Session {
	return &Session{}
}

// Send creates a packet for the given body and updates session state
func (s *Session) Send(body *Body) *Packet {
	s.mu.Lock()
	defer s.mu.Unlock()

	header := &Header{
		Sent: s.sent,
		Recv: s.recv,
		LMID: s.id,
		RMID: s.rmid,
	}

	// Calculate PID based on body type
	switch body.Type {
	case BodyTypeSync:
		header.PID = 0x0002FFFF
	default:
		header.PID = 0x0000FFFF - s.count
	}

	// Update counters
	s.sent += uint32(body.Len())
	s.id++

	// Increment count for all non-sync packets so each gets a unique PID.
	// The device uses PID for deduplication and will discard duplicate PIDs.
	if body.Type != BodyTypeSync {
		s.count++
	}

	return NewPacket(header, body)
}

// Recv processes a received packet and updates session state
func (s *Session) Recv(packet *Packet) *Packet {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recv += uint32(packet.Body.Len())
	s.rmid = packet.Header.LMID

	return packet
}

// GetState returns the current session state
func (s *Session) GetState() (sent, recv, count, id, rmid uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent, s.recv, s.count, s.id, s.rmid
}

// Reset resets the session state
func (s *Session) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = 0
	s.recv = 0
	s.count = 0
	s.id = 0
	s.rmid = 0
}
