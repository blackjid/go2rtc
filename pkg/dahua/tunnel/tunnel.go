// Package tunnel provides TCP tunneling over PTCP protocol
package tunnel

import (
	"context"
	"errors"
	"io"
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
	maxMissedHeartbeats     = 3
)

// Tunnel represents a P2P tunnel to a Dahua device
type Tunnel struct {
	mu      sync.RWMutex
	client  *dh.UDPClient
	session *ptcp.Session
	closed  bool
	done    chan struct{} // closed on shutdown; used by reader and heartbeat

	// Realms map connection IDs to their data channels
	realms      map[uint32]chan []byte
	realmsMu    sync.RWMutex
	nextRealmID uint32

	// For connection establishment
	connCh   map[uint32]chan bool
	connChMu sync.Mutex

	// Heartbeat
	heartbeatTicker *time.Ticker

	// Health tracking
	missedHeartbeats    int
	missedHeartbeatsMu  sync.Mutex
	consecutiveReadErrs int
	consecutiveSendErrs int

	// OnClose is called once when the tunnel closes (heartbeat timeout, errors, etc.)
	OnClose func()
}

// Config contains tunnel configuration
type Config struct {
	Serial     string
	Username   string
	Password   string
	RemotePort uint32 // Default: 554 for RTSP
	RelayMode  bool
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
		RelayMode:      cfg.RelayMode,
		Timeout:        cfg.Timeout,
		DeviceUsername:  cfg.Username,
		DevicePassword: cfg.Password,
		P2PPort:        cfg.P2PPort,
	})
	if err != nil {
		return nil, err
	}

	t := &Tunnel{
		client:      result.Client,
		session:     result.Session,
		done:        make(chan struct{}),
		realms:      make(map[uint32]chan []byte),
		nextRealmID: 1,
		connCh:      make(map[uint32]chan bool),
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
	t.missedHeartbeatsMu.Lock()
	t.missedHeartbeats = 0
	t.missedHeartbeatsMu.Unlock()
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
			continue
		}

		t.session.Recv(packet)

		switch packet.Body.Type {
		case ptcp.BodyTypeEmpty:
			continue

		case ptcp.BodyTypeStatus:
			realm := packet.Body.Realm
			status := packet.Body.Status

			if status == ptcp.StatusConnect {
				t.connChMu.Lock()
				if ch, ok := t.connCh[realm]; ok {
					ch <- true
					delete(t.connCh, realm)
				}
				t.connChMu.Unlock()
			}

			t.sendACK()

		case ptcp.BodyTypePayload:
			realm := packet.Body.Realm
			t.realmsMu.RLock()
			ch, ok := t.realms[realm]
			t.realmsMu.RUnlock()

			if ok {
				select {
				case ch <- packet.Body.Data:
				case <-t.done:
				case <-time.After(100 * time.Millisecond):
					log.Warn().Uint32("realm", realm).Msg("[dahua] payload channel full, dropping packet")
				}
			}

			t.sendACK()

		case ptcp.BodyTypeHeartbeat:
			t.sendACK()
		}
	}
}

// sendACK sends an empty ACK packet and tracks consecutive failures.
func (t *Tunnel) sendACK() {
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

// Dial creates a new realm/connection to the specified port on the device.
// Retries the bind request up to 3 times since UDP packets can be lost.
func (t *Tunnel) Dial(port uint32) (*Conn, error) {
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return nil, ErrTunnelClosed
	}
	t.mu.RUnlock()

	dataCh := make(chan []byte, 4096)
	connCh := make(chan bool, 1)

	t.realmsMu.Lock()
	realmID := t.nextRealmID
	t.nextRealmID++
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

	const maxRetries = 3
	const retryTimeout = 5 * time.Second

	connected := false
	for attempt := 0; attempt < maxRetries; attempt++ {
		bindPacket := t.session.Send(ptcp.NewBindBody(realmID, port))
		if err := t.client.Send(bindPacket.Serialize()); err != nil {
			cleanup()
			return nil, err
		}

		select {
		case <-connCh:
			connected = true
		case <-t.done:
			cleanup()
			return nil, ErrTunnelClosed
		case <-time.After(retryTimeout):
			log.Debug().Int("attempt", attempt+1).Uint32("port", port).Msg("[dahua] bind retry")
			continue
		}

		if connected {
			break
		}
	}

	if !connected {
		cleanup()
		log.Debug().Uint32("port", port).Uint32("realm", realmID).Bool("closed", t.IsClosed()).Msg("[dahua] bind request timed out after all retries")
		return nil, ErrDialTimeout
	}

	return &Conn{
		tunnel:  t,
		realmID: realmID,
		dataCh:  dataCh,
		closeCh: make(chan struct{}),
	}, nil
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

	// Create a tunnel connection
	tunnelConn, err := l.tunnel.Dial(l.port)
	if err != nil {
		log.Warn().Err(err).Msg("[dahua] failed to dial tunnel")
		return
	}
	defer tunnelConn.Close()

	// Bidirectional copy with half-close propagation:
	// when one direction ends, close the other side to unblock it.
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

// Close closes the listener
func (l *Listener) Close() error {
	l.cancel()
	return l.listener.Close()
}
