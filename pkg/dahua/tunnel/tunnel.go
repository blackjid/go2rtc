// Package tunnel provides TCP tunneling over PTCP protocol
package tunnel

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/dahua/dh"
	"github.com/AlexxIT/go2rtc/pkg/dahua/ptcp"
	"github.com/rs/zerolog/log"
)

// Errors
var (
	ErrTunnelClosed = errors.New("tunnel is closed")
	ErrNotConnected = errors.New("not connected")
	ErrDialTimeout  = errors.New("dial timeout: device not responding")
)

// Tunnel health thresholds
const (
	maxConsecutiveReadErrors = 10
	maxConsecutiveSendErrors = 5
	maxMissedHeartbeats     = 24 // 2 minutes; RTSP read timeout provides faster detection
)

// Tunnel represents a P2P tunnel to a Dahua device
type Tunnel struct {
	mu      sync.RWMutex
	client  *dh.UDPClient
	session *ptcp.Session
	closed  bool
	done    chan struct{} // closed on shutdown; used by reader and heartbeat

	// Realms map connection IDs to their data channels.
	// Realm IDs are random uint32 values, matching DMSS app behavior.
	realms   map[uint32]chan []byte
	realmsMu sync.RWMutex

	// For connection establishment
	connCh   map[uint32]chan bool
	connChMu sync.Mutex
	dialMu   sync.Mutex // serializes Dial calls so BINDs don't overlap

	// Heartbeat
	heartbeatTicker *time.Ticker

	// Health tracking
	missedHeartbeats    int
	missedHeartbeatsMu  sync.Mutex
	consecutiveReadErrs int
	consecutiveSendErrs int

	lastAckTime   time.Time
	lastAckTimeMu sync.Mutex

	// ACK coalescing: batch multiple received packets into a single ACK
	ackPending bool
	ackTimer   *time.Timer
	ackMu      sync.Mutex

	// OnClose is called once when the tunnel closes (heartbeat timeout, errors, etc.)
	OnClose func()
}

// Config contains tunnel configuration
type Config struct {
	Serial     string
	Username   string
	Password   string
	RemotePort uint32 // Default: 554 for RTSP
	Timeout    time.Duration
	P2PPort    int // Fixed local UDP port for P2P (0 = random)
}

// New creates a new tunnel with the given configuration
func New(cfg Config) (*Tunnel, error) {
	if cfg.RemotePort == 0 {
		cfg.RemotePort = 554
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}

	result, err := dh.Handshake(dh.HandshakeOptions{
		Serial:         cfg.Serial,
		Timeout:        cfg.Timeout,
		DeviceUsername:  cfg.Username,
		DevicePassword: cfg.Password,
		P2PPort:        cfg.P2PPort,
	})
	if err != nil {
		return nil, err
	}

	t := &Tunnel{
		client:  result.Client,
		session: result.Session,
		done:    make(chan struct{}),
		realms:  make(map[uint32]chan []byte),
		connCh:  make(map[uint32]chan bool),
	}

	// Start reader goroutine
	go t.reader()

	// Start heartbeat
	t.startHeartbeat()

	return t, nil
}

// Close closes the tunnel, sending DISC for all active realms first.
func (t *Tunnel) Close() error {
	t.mu.Lock()

	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.done)

	if t.heartbeatTicker != nil {
		t.heartbeatTicker.Stop()
	}

	t.ackMu.Lock()
	if t.ackTimer != nil {
		t.ackTimer.Stop()
	}
	t.ackMu.Unlock()

	// Collect active realm IDs and send DISC for each before closing the socket
	t.realmsMu.Lock()
	realmIDs := make([]uint32, 0, len(t.realms))
	for id := range t.realms {
		realmIDs = append(realmIDs, id)
	}
	t.realms = nil
	t.realmsMu.Unlock()

	for _, id := range realmIDs {
		packet := t.session.Send(ptcp.NewStatusBody(id, ptcp.StatusDisconnect))
		t.client.Send(packet.Serialize())
	}

	err := t.client.Close()
	onClose := t.OnClose
	t.mu.Unlock()

	if onClose != nil {
		onClose()
	}
	return err
}

// IsClosed returns whether the tunnel is closed
func (t *Tunnel) IsClosed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.closed
}

// startHeartbeat starts the heartbeat ticker
func (t *Tunnel) startHeartbeat() {
	t.heartbeatTicker = time.NewTicker(5 * time.Second)

	go func() {
		for {
			select {
			case <-t.heartbeatTicker.C:
				t.sendHeartbeat()
			case <-t.done:
				return
			}
		}
	}()
}

// sendHeartbeat sends a heartbeat packet and tracks missed responses.
func (t *Tunnel) sendHeartbeat() {
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return
	}
	t.mu.RUnlock()

	t.missedHeartbeatsMu.Lock()
	t.missedHeartbeats++
	missed := t.missedHeartbeats
	t.missedHeartbeatsMu.Unlock()

	if missed > maxMissedHeartbeats {
		log.Warn().Int("missed", missed).Msg("[dahua] too many missed heartbeats, closing tunnel")
		t.Close()
		return
	}

	packet := t.session.Send(ptcp.NewHeartbeatBody())
	if err := t.client.Send(packet.Serialize()); err != nil {
		log.Warn().Err(err).Msg("[dahua] heartbeat send failed")
	}
}

// ackHeartbeat resets the missed heartbeat counter (called when any packet is received).
func (t *Tunnel) ackHeartbeat() {
	now := time.Now()
	t.missedHeartbeatsMu.Lock()
	t.missedHeartbeats = 0
	t.missedHeartbeatsMu.Unlock()
	t.lastAckTimeMu.Lock()
	t.lastAckTime = now
	t.lastAckTimeMu.Unlock()
}

// IsResponsive returns true if the tunnel received a packet from the device
// within the given duration. This indicates the device is not overloaded
// and can handle new BIND/DESCRIBE requests.
func (t *Tunnel) IsResponsive(within time.Duration) bool {
	t.lastAckTimeMu.Lock()
	last := t.lastAckTime
	t.lastAckTimeMu.Unlock()
	if last.IsZero() {
		return false
	}
	return time.Since(last) < within
}

// reader reads packets from the device
func (t *Tunnel) reader() {
	for {
		select {
		case <-t.done:
			return
		default:
		}

		data, err := t.client.ReadRaw(1 * time.Second)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			select {
			case <-t.done:
				return
			default:
			}

			t.consecutiveReadErrs++
			if t.consecutiveReadErrs >= maxConsecutiveReadErrors {
				log.Warn().Int("errors", t.consecutiveReadErrs).Msg("[dahua] too many read errors, closing tunnel")
				t.Close()
				return
			}
			continue
		}

		t.consecutiveReadErrs = 0
		t.ackHeartbeat()

		packet, err := ptcp.ParsePacket(data)
		if err != nil {
			log.Debug().Err(err).Int("raw_len", len(data)).Msg("[dahua] reader: unparseable packet")
			continue
		}

		t.session.Recv(packet)

		switch packet.Body.Type {
		case ptcp.BodyTypeEmpty:
			// Don't ACK empty packets; they are ACKs themselves.
			continue

		case ptcp.BodyTypeSync:
			syncPkt := t.session.Send(ptcp.NewSyncBody())
			t.client.Send(syncPkt.Serialize())
			continue

		case ptcp.BodyTypeStatus:
			realm := packet.Body.Realm
			status := packet.Body.Status

			log.Debug().Uint32("realm", realm).Str("status", status).Msg("[dahua] PTCP status")

			if status == ptcp.StatusConnect {
				t.connChMu.Lock()
				if ch, ok := t.connCh[realm]; ok {
					ch <- true
					delete(t.connCh, realm)
				} else {
					log.Warn().Uint32("realm", realm).Msg("[dahua] reader: StatusConnect for unknown realm")
				}
				t.connChMu.Unlock()
			}

			t.scheduleACK()

		case ptcp.BodyTypePayload:
			realm := packet.Body.Realm
			t.realmsMu.RLock()
			ch, ok := t.realms[realm]
			t.realmsMu.RUnlock()

			if ok {
				select {
				case ch <- packet.Body.Data:
				case <-t.done:
				case <-time.After(500 * time.Millisecond):
					log.Warn().Uint32("realm", realm).Msg("[dahua] payload channel full, dropping packet")
				}
			} else {
				log.Warn().Uint32("realm", realm).Int("data_len", len(packet.Body.Data)).Msg("[dahua] reader: payload for unknown realm")
			}

			t.scheduleACK()

		case ptcp.BodyTypeHeartbeat:
			t.scheduleACK()

		default:
			log.Debug().Int("body_type", int(packet.Body.Type)).Msg("[dahua] reader: unhandled body type")
		}
	}
}

// ackCoalesceWindow is how long to wait before sending a batched ACK.
// The DMSS app averages ~2.7 payloads per ACK; 20ms coalesces effectively
// while staying responsive.
const ackCoalesceWindow = 20 * time.Millisecond

// scheduleACK marks that an ACK is needed and starts a coalescing timer.
// Multiple received packets within the window produce a single ACK,
// reducing traffic to the device.
func (t *Tunnel) scheduleACK() {
	t.ackMu.Lock()
	defer t.ackMu.Unlock()

	if t.ackPending {
		return
	}
	t.ackPending = true
	t.ackTimer = time.AfterFunc(ackCoalesceWindow, t.flushACK)
}

// flushACK sends the coalesced ACK.
func (t *Tunnel) flushACK() {
	t.ackMu.Lock()
	t.ackPending = false
	t.ackMu.Unlock()

	ackPacket := t.session.Send(ptcp.NewEmptyBody())
	if err := t.client.Send(ackPacket.Serialize()); err != nil {
		t.consecutiveSendErrs++
		if t.consecutiveSendErrs >= maxConsecutiveSendErrors {
			log.Warn().Int("errors", t.consecutiveSendErrs).Msg("[dahua] too many ACK send errors, closing tunnel")
			t.Close()
			return
		}
		log.Debug().Err(err).Msg("[dahua] ACK send failed")
		return
	}
	t.consecutiveSendErrs = 0
}

// randomRealmID generates a random uint32 realm ID that doesn't collide with
// any existing realm. The DMSS app uses large, non-sequential realm IDs.
func (t *Tunnel) randomRealmID() uint32 {
	t.realmsMu.RLock()
	defer t.realmsMu.RUnlock()
	for {
		id := rand.Uint32()
		if _, exists := t.realms[id]; !exists {
			return id
		}
	}
}

// ActiveRealms returns the number of currently active realms on this tunnel.
func (t *Tunnel) ActiveRealms() int {
	t.realmsMu.RLock()
	n := len(t.realms)
	t.realmsMu.RUnlock()
	return n
}

// Dial creates a new realm/connection to the specified port on the device.
// Serialized via dialMu so the device processes one BIND at a time.
// Retries the bind request up to 3 times since UDP packets can be lost.
func (t *Tunnel) Dial(port uint32) (*Conn, error) {
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return nil, ErrTunnelClosed
	}
	t.mu.RUnlock()

	// Serialize Dial calls: the device can't reliably handle concurrent BINDs
	t.dialMu.Lock()
	defer t.dialMu.Unlock()

	if t.IsClosed() {
		return nil, ErrTunnelClosed
	}

	const maxRetries = 3
	const retryTimeout = 5 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		realmID := t.randomRealmID()
		dataCh := make(chan []byte, 4096)
		connCh := make(chan bool, 1)

		t.realmsMu.Lock()
		t.realms[realmID] = dataCh
		t.realmsMu.Unlock()

		t.connChMu.Lock()
		t.connCh[realmID] = connCh
		t.connChMu.Unlock()

		cleanup := func() {
			t.realmsMu.Lock()
			delete(t.realms, realmID)
			t.realmsMu.Unlock()
			t.connChMu.Lock()
			delete(t.connCh, realmID)
			t.connChMu.Unlock()
		}

		bindBody := ptcp.NewBindBody(realmID, port)
		bindPacket := t.session.Send(bindBody)

		log.Debug().
			Int("attempt", attempt+1).
			Uint32("realm", realmID).
			Uint32("port", port).
			Msg("[dahua] sending BIND")

		if err := t.client.Send(bindPacket.Serialize()); err != nil {
			cleanup()
			return nil, err
		}

		select {
		case <-connCh:
			return &Conn{
				tunnel:  t,
				realmID: realmID,
				dataCh:  dataCh,
				closeCh: make(chan struct{}),
			}, nil
		case <-t.done:
			cleanup()
			return nil, ErrTunnelClosed
		case <-time.After(retryTimeout):
			cleanup()
			log.Debug().Int("attempt", attempt+1).Uint32("port", port).Uint32("realm", realmID).Msg("[dahua] bind retry with fresh realm ID")
			continue
		}
	}

	log.Warn().Uint32("port", port).Bool("closed", t.IsClosed()).Msg("[dahua] bind request timed out after all retries")
	return nil, ErrDialTimeout
}

// Conn represents a connection within a tunnel
type Conn struct {
	tunnel  *Tunnel
	realmID uint32
	dataCh  chan []byte
	closeCh chan struct{} // closed by Close() to unblock Read()
	closed  bool
	mu      sync.Mutex

	readBuf      []byte
	readDeadline time.Time
}

// Read reads data from the connection, respecting SetReadDeadline.
func (c *Conn) Read(p []byte) (n int, err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, io.EOF
	}

	if len(c.readBuf) > 0 {
		n = copy(p, c.readBuf)
		c.readBuf = c.readBuf[n:]
		c.mu.Unlock()
		return n, nil
	}

	deadline := c.readDeadline
	c.mu.Unlock()

	var timer <-chan time.Time
	if !deadline.IsZero() {
		d := time.Until(deadline)
		if d <= 0 {
			return 0, os.ErrDeadlineExceeded
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timer = t.C
	}

	select {
	case data, ok := <-c.dataCh:
		if !ok {
			return 0, io.EOF
		}
		n = copy(p, data)
		if n < len(data) {
			c.mu.Lock()
			c.readBuf = append(c.readBuf, data[n:]...)
			c.mu.Unlock()
		}
		return n, nil
	case <-c.closeCh:
		return 0, io.EOF
	case <-timer:
		return 0, os.ErrDeadlineExceeded
	}
}

// maxPayloadSize is the maximum PTCP payload per UDP packet.
// PTCP header (24) + body header (12) + 1280 = 1316 bytes UDP payload,
// matching the DMSS app and staying well within MTU limits.
const maxPayloadSize = 1280

// Write writes data to the connection, fragmenting into MTU-safe PTCP packets.
func (c *Conn) Write(p []byte) (n int, err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, ErrTunnelClosed
	}
	c.mu.Unlock()

	if c.tunnel.IsClosed() {
		return 0, ErrTunnelClosed
	}

	for len(p) > 0 {
		chunk := p
		if len(chunk) > maxPayloadSize {
			chunk = p[:maxPayloadSize]
		}
		p = p[len(chunk):]

		packet := c.tunnel.session.Send(ptcp.NewPayloadBody(c.realmID, chunk))
		if err := c.tunnel.client.Send(packet.Serialize()); err != nil {
			return n, err
		}
		n += len(chunk)
	}

	return n, nil
}

// Close closes the connection, sending DISC with retries for UDP reliability.
func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.closeCh)
	c.mu.Unlock()

	if !c.tunnel.IsClosed() {
		const discRetries = 3
		const discInterval = 50 * time.Millisecond
		for i := 0; i < discRetries; i++ {
			packet := c.tunnel.session.Send(ptcp.NewStatusBody(c.realmID, ptcp.StatusDisconnect))
			if err := c.tunnel.client.Send(packet.Serialize()); err != nil {
				break
			}
			if i < discRetries-1 {
				time.Sleep(discInterval)
			}
		}
	}

	c.tunnel.realmsMu.Lock()
	delete(c.tunnel.realms, c.realmID)
	c.tunnel.realmsMu.Unlock()

	return nil
}

// LocalAddr returns the local address (placeholder)
func (c *Conn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
}

// RemoteAddr returns the remote address (placeholder)
func (c *Conn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(0, 0, 0, 0), Port: 554}
}

// SetDeadline sets the read deadline (write deadline is not supported).
func (c *Conn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return nil
}

// SetReadDeadline sets the read deadline.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return nil
}

// SetWriteDeadline is a no-op (writes are non-blocking UDP sends).
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return nil
}

// Listener creates a local TCP listener that tunnels to the device
type Listener struct {
	tunnel   *Tunnel
	listener net.Listener
	port     uint32
	ctx      context.Context
	cancel   context.CancelFunc

	connsMu sync.Mutex
	conns   map[*Conn]net.Conn // tunnel conn → TCP conn, for cleanup on Close
}

// Listen creates a new listener on the specified local address
func (t *Tunnel) Listen(localAddr string, remotePort uint32) (*Listener, error) {
	ln, err := net.Listen("tcp", localAddr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	l := &Listener{
		tunnel:   t,
		listener: ln,
		port:     remotePort,
		ctx:      ctx,
		cancel:   cancel,
		conns:    make(map[*Conn]net.Conn),
	}

	go l.acceptLoop()

	return l, nil
}

// acceptLoop accepts TCP connections and tunnels them
func (l *Listener) acceptLoop() {
	for {
		select {
		case <-l.ctx.Done():
			return
		default:
		}

		conn, err := l.listener.Accept()
		if err != nil {
			continue
		}

		go l.handleConn(conn)
	}
}

// handleConn handles a single TCP connection
func (l *Listener) handleConn(tcpConn net.Conn) {
	defer tcpConn.Close()

	tunnelConn, err := l.tunnel.Dial(l.port)
	if err != nil {
		log.Warn().Err(err).Msg("[dahua] failed to dial tunnel")
		return
	}
	defer tunnelConn.Close()

	l.connsMu.Lock()
	l.conns[tunnelConn] = tcpConn
	l.connsMu.Unlock()

	defer func() {
		l.connsMu.Lock()
		delete(l.conns, tunnelConn)
		l.connsMu.Unlock()
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(tunnelConn, tcpConn)
		tunnelConn.Close()
	}()

	go func() {
		defer wg.Done()
		io.Copy(tcpConn, tunnelConn)
		tcpConn.Close()
	}()

	wg.Wait()
}

// Addr returns the listener address
func (l *Listener) Addr() net.Addr {
	return l.listener.Addr()
}

// Close closes the listener and all active tunneled connections.
// This sends DISC for each realm so the device frees resources immediately.
func (l *Listener) Close() error {
	l.cancel()
	err := l.listener.Close()

	l.connsMu.Lock()
	for tunnelConn, tcpConn := range l.conns {
		tunnelConn.Close()
		tcpConn.Close()
	}
	l.conns = nil
	l.connsMu.Unlock()

	return err
}
