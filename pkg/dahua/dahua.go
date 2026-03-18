// Package dahua provides a Go implementation of the Dahua P2P protocol
// for connecting to Dahua/KBVision cameras through their P2P service.
package dahua

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/dahua/dh"
	"github.com/AlexxIT/go2rtc/pkg/dahua/tunnel"
	"github.com/rs/zerolog/log"
)

// Client represents a P2P connection to a Dahua device
type Client struct {
	tunnel      *tunnel.Tunnel
	config      Config
	NegotiateMu sync.Mutex // serializes RTSP negotiation; device can't handle concurrent SETUP
}

// SetOnClose registers a callback invoked when the underlying tunnel closes
// (e.g. heartbeat timeout, too many errors). Must be called before any I/O.
func (c *Client) SetOnClose(fn func()) {
	c.tunnel.OnClose = fn
}

// Config contains configuration options for the P2P client
type Config struct {
	Serial   string
	Username string
	Password string
	Timeout  time.Duration
	P2PPort  int // Fixed local UDP port for P2P (0 = random)
}

// ConnectWithConfig creates a new P2P connection with custom configuration
func ConnectWithConfig(cfg Config) (*Client, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}

	t, err := tunnel.New(tunnel.Config{
		Serial:   cfg.Serial,
		Username: cfg.Username,
		Password: cfg.Password,
		Timeout:  cfg.Timeout,
		P2PPort:  cfg.P2PPort,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		tunnel: t,
		config: cfg,
	}, nil
}

// Close closes the P2P connection
func (c *Client) Close() error {
	return c.tunnel.Close()
}

// IsClosed returns whether the tunnel is closed
func (c *Client) IsClosed() bool {
	return c.tunnel.IsClosed()
}

// Listen creates a local TCP server that tunnels connections to the device
func (c *Client) Listen(localAddr string, remotePort uint32) (*tunnel.Listener, error) {
	return c.tunnel.Listen(localAddr, remotePort)
}

// Errors re-exported from subpackages
var (
	ErrDeviceOffline        = dh.ErrDeviceOffline
	ErrDeviceNotFound       = dh.ErrDeviceNotFound
	ErrAuthenticationFailed = dh.ErrAuthenticationFailed
	ErrTimeout              = dh.ErrTimeout
	ErrTunnelClosed         = tunnel.ErrTunnelClosed
)

// DefaultIdleTimeout is how long a tunnel persists after all streams disconnect.
const DefaultIdleTimeout = 5 * time.Minute

// HandshakeCooldown prevents hammering P2P servers after repeated failures.
const HandshakeCooldown = 30 * time.Second

// SessionManager manages shared P2P tunnels per serial with refcounting.
// Tunnels persist for IdleTimeout after the last stream disconnects,
// avoiding expensive P2P re-handshakes on reconnect.
type SessionManager struct {
	mu          sync.Mutex
	sessions    map[string]*managedSession
	inflight    map[string]*inflightConn
	lastFailed  map[string]time.Time // per-serial cooldown after handshake failures
	IdleTimeout time.Duration
}

type managedSession struct {
	client    *Client
	refCount  int
	idleTimer *time.Timer
}

// inflightConn tracks an in-progress handshake so multiple goroutines
// don't race to create the same tunnel.
type inflightConn struct {
	done   chan struct{} // closed when handshake finishes
	client *Client
	err    error
}

// NewSessionManager creates a new session manager
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions:    make(map[string]*managedSession),
		inflight:    make(map[string]*inflightConn),
		lastFailed:  make(map[string]time.Time),
		IdleTimeout: DefaultIdleTimeout,
	}
}

// Acquire returns a cached or new client for the given config.
// The caller must call Release when done.
// The handshake runs outside the lock so other channels are not blocked.
func (m *SessionManager) Acquire(cfg Config) (*Client, error) {
	m.mu.Lock()

	// Enforce cooldown after recent failures
	if lastFail, ok := m.lastFailed[cfg.Serial]; ok {
		remaining := HandshakeCooldown - time.Since(lastFail)
		if remaining > 0 {
			m.mu.Unlock()
			log.Debug().Str("serial", cfg.Serial).Dur("remaining", remaining).Msg("[dahua] acquire blocked by cooldown")
			return nil, fmt.Errorf("connection cooldown active (%s remaining)", remaining.Round(time.Second))
		}
		delete(m.lastFailed, cfg.Serial)
	}

	// Return existing live session
	if s, ok := m.sessions[cfg.Serial]; ok {
		if s.client.IsClosed() {
			if s.idleTimer != nil {
				s.idleTimer.Stop()
			}
			delete(m.sessions, cfg.Serial)
		} else {
			if s.idleTimer != nil {
				s.idleTimer.Stop()
				s.idleTimer = nil
			}
			s.refCount++
			m.mu.Unlock()
			return s.client, nil
		}
	}

	// If another goroutine is already connecting, wait for it
	if inf, ok := m.inflight[cfg.Serial]; ok {
		m.mu.Unlock()
		<-inf.done
		if inf.err != nil {
			return nil, inf.err
		}
		// Session was just created — grab it
		m.mu.Lock()
		if s, ok := m.sessions[cfg.Serial]; ok && !s.client.IsClosed() {
			s.refCount++
			m.mu.Unlock()
			return s.client, nil
		}
		m.mu.Unlock()
		return nil, ErrTimeout
	}

	// Register in-flight handshake and release lock
	inf := &inflightConn{done: make(chan struct{})}
	m.inflight[cfg.Serial] = inf
	m.mu.Unlock()

	// Run handshake WITHOUT holding the lock
	client, err := ConnectWithConfig(cfg)
	inf.client = client
	inf.err = err

	m.mu.Lock()
	delete(m.inflight, cfg.Serial)
	if err == nil {
		ms := &managedSession{
			client:   client,
			refCount: 1,
		}
		m.sessions[cfg.Serial] = ms

		serial := cfg.Serial
		client.SetOnClose(func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			if cur, ok := m.sessions[serial]; ok && cur == ms {
				if cur.idleTimer != nil {
					cur.idleTimer.Stop()
				}
				delete(m.sessions, serial)
			}
		})

		delete(m.lastFailed, cfg.Serial)
	}
	m.mu.Unlock()

	// Wake all waiters
	close(inf.done)

	if err != nil {
		return nil, err
	}
	return client, nil
}

// Invalidate force-closes and removes the session for the given serial.
// Use this when the tunnel is confirmed broken (e.g. Dial/RTSP timeouts)
// so the next Acquire creates a fresh tunnel instead of reusing the dead one.
// A cooldown is applied to prevent hammering the device with reconnects.
func (m *SessionManager) Invalidate(serial string) {
	m.mu.Lock()
	s, ok := m.sessions[serial]
	if !ok {
		m.mu.Unlock()
		return
	}
	if s.idleTimer != nil {
		s.idleTimer.Stop()
	}
	delete(m.sessions, serial)
	m.lastFailed[serial] = time.Now()
	m.mu.Unlock()

	log.Warn().Str("serial", serial).Dur("cooldown", HandshakeCooldown).Msg("[dahua] session invalidated, cooldown applied")
	s.client.Close()
}

// Release decrements the refcount for the given serial.
// When refcount reaches zero, the tunnel is kept alive for IdleTimeout
// before being closed. A new Acquire within that window reuses it.
func (m *SessionManager) Release(serial string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.sessions[serial]
	if !ok {
		return
	}

	s.refCount--
	if s.refCount <= 0 {
		s.refCount = 0
		if s.idleTimer != nil {
			s.idleTimer.Stop()
		}
		s.idleTimer = time.AfterFunc(m.IdleTimeout, func() {
			m.mu.Lock()
			s2, ok := m.sessions[serial]
			if !ok || s2 != s || s2.refCount > 0 {
				m.mu.Unlock()
				return
			}
			delete(m.sessions, serial)
			m.mu.Unlock()

			// Close outside the lock to avoid deadlock with OnClose callback
			s2.client.Close()
		})
	}
}

// Dial creates a new connection to the specified port on the device (via tunnel)
func (c *Client) Dial(port uint32) (net.Conn, error) {
	return c.tunnel.Dial(port)
}

// CloseAll closes all active sessions immediately.
// Call this on process shutdown to notify devices of disconnection.
func (m *SessionManager) CloseAll() {
	m.mu.Lock()
	sessions := make([]*managedSession, 0, len(m.sessions))
	for serial, s := range m.sessions {
		if s.idleTimer != nil {
			s.idleTimer.Stop()
		}
		delete(m.sessions, serial)
		sessions = append(sessions, s)
	}
	m.mu.Unlock()

	for _, s := range sessions {
		s.client.Close()
	}
}
