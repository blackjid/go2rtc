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

// NegotiateSettle is the maximum settle time between consecutive RTSP
// negotiations. Adaptive settling will reduce this if the device is responsive.
const NegotiateSettle = 5 * time.Second

// minNegotiateSettle is the minimum settle time used when the device is
// actively responding to heartbeats (indicating low load).
const minNegotiateSettle = 1 * time.Second

// Client represents a P2P connection to a Dahua device
type Client struct {
	tunnel        *tunnel.Tunnel
	config        Config
	NegotiateMu   sync.Mutex // serializes RTSP negotiation; device can't handle concurrent SETUP
	lastNegotiate time.Time  // when the last RTSP negotiation completed
}

// WaitSettle sleeps until the device is ready for a new RTSP negotiation.
// It uses adaptive delays: if the tunnel is receiving packets from the
// device (responsive), the settle time is reduced from 5s to 1s. Otherwise,
// the full NegotiateSettle is enforced.
func (c *Client) WaitSettle() {
	if c.lastNegotiate.IsZero() {
		return
	}

	settle := NegotiateSettle
	if c.tunnel.IsResponsive(2 * time.Second) {
		settle = minNegotiateSettle
	}

	if wait := settle - time.Since(c.lastNegotiate); wait > 0 {
		time.Sleep(wait)
	}
}

// DoneNegotiate records the current time as the end of an RTSP negotiation.
func (c *Client) DoneNegotiate() {
	c.lastNegotiate = time.Now()
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

// ActiveRealms returns the number of active realms on the underlying tunnel.
func (c *Client) ActiveRealms() int {
	return c.tunnel.ActiveRealms()
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

// MaxRealmsPerTunnel is the maximum number of realms to multiplex on a single
// P2P tunnel before opening a second tunnel. Dahua firmware becomes unstable
// beyond 3-5 concurrent realms; 4 is a safe default.
const MaxRealmsPerTunnel = 4

// SessionManager manages shared P2P tunnels per serial with refcounting.
// Multiple tunnels can be opened per serial to work around the device's
// per-tunnel realm limit. Tunnels persist for IdleTimeout after the last
// stream disconnects, avoiding expensive P2P re-handshakes on reconnect.
type SessionManager struct {
	mu          sync.Mutex
	sessions    map[string][]*managedSession // serial -> list of tunnels
	inflight    map[string]*inflightConn
	lastFailed  map[string]time.Time
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
	done   chan struct{}
	client *Client
	err    error
}

// NewSessionManager creates a new session manager
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions:    make(map[string][]*managedSession),
		inflight:    make(map[string]*inflightConn),
		lastFailed:  make(map[string]time.Time),
		IdleTimeout: DefaultIdleTimeout,
	}
}

// findAvailableSession returns an existing live session with capacity, or nil.
// Must be called with m.mu held.
func (m *SessionManager) findAvailableSession(serial string) *managedSession {
	sessions := m.sessions[serial]
	live := sessions[:0]
	for _, s := range sessions {
		if s.client.IsClosed() {
			if s.idleTimer != nil {
				s.idleTimer.Stop()
			}
			continue
		}
		live = append(live, s)
	}
	m.sessions[serial] = live

	for _, s := range live {
		if s.client.ActiveRealms() < MaxRealmsPerTunnel {
			return s
		}
	}
	return nil
}

// Acquire returns a cached or new client for the given config.
// If all existing tunnels are at capacity, a new tunnel is created.
// The caller must call Release when done.
func (m *SessionManager) Acquire(cfg Config) (*Client, error) {
	m.mu.Lock()

	if lastFail, ok := m.lastFailed[cfg.Serial]; ok {
		remaining := HandshakeCooldown - time.Since(lastFail)
		if remaining > 0 {
			m.mu.Unlock()
			log.Debug().Str("serial", cfg.Serial).Dur("remaining", remaining).Msg("[dahua] acquire blocked by cooldown")
			return nil, fmt.Errorf("connection cooldown active (%s remaining)", remaining.Round(time.Second))
		}
		delete(m.lastFailed, cfg.Serial)
	}

	if s := m.findAvailableSession(cfg.Serial); s != nil {
		if s.idleTimer != nil {
			s.idleTimer.Stop()
			s.idleTimer = nil
		}
		s.refCount++
		m.mu.Unlock()
		return s.client, nil
	}

	if inf, ok := m.inflight[cfg.Serial]; ok {
		m.mu.Unlock()
		<-inf.done
		if inf.err != nil {
			return nil, inf.err
		}
		m.mu.Lock()
		if s := m.findAvailableSession(cfg.Serial); s != nil {
			s.refCount++
			m.mu.Unlock()
			return s.client, nil
		}
		m.mu.Unlock()
		return nil, ErrTimeout
	}

	inf := &inflightConn{done: make(chan struct{})}
	m.inflight[cfg.Serial] = inf
	m.mu.Unlock()

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
		m.sessions[cfg.Serial] = append(m.sessions[cfg.Serial], ms)

		serial := cfg.Serial
		client.SetOnClose(func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			sessions := m.sessions[serial]
			for i, cur := range sessions {
				if cur == ms {
					if cur.idleTimer != nil {
						cur.idleTimer.Stop()
					}
					m.sessions[serial] = append(sessions[:i], sessions[i+1:]...)
					break
				}
			}
			if len(m.sessions[serial]) == 0 {
				delete(m.sessions, serial)
			}
		})

		delete(m.lastFailed, cfg.Serial)
	}
	m.mu.Unlock()

	close(inf.done)

	if err != nil {
		return nil, err
	}
	return client, nil
}

// Invalidate force-closes all tunnels for the given serial.
// Use this when a tunnel is confirmed broken so the next Acquire
// creates a fresh tunnel. A cooldown is applied.
func (m *SessionManager) Invalidate(serial string) {
	m.mu.Lock()
	sessions := m.sessions[serial]
	delete(m.sessions, serial)
	m.lastFailed[serial] = time.Now()
	m.mu.Unlock()

	if len(sessions) == 0 {
		return
	}

	log.Warn().Str("serial", serial).Int("tunnels", len(sessions)).Dur("cooldown", HandshakeCooldown).Msg("[dahua] session invalidated, cooldown applied")
	for _, s := range sessions {
		if s.idleTimer != nil {
			s.idleTimer.Stop()
		}
		s.client.Close()
	}
}

// Release decrements the refcount for the specific client under the given serial.
// When refcount reaches zero, the tunnel is kept alive for IdleTimeout
// before being closed.
func (m *SessionManager) Release(serial string, client *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, s := range m.sessions[serial] {
		if s.client == client {
			s.refCount--
			if s.refCount <= 0 {
				s.refCount = 0
				if s.idleTimer != nil {
					s.idleTimer.Stop()
				}
				s.idleTimer = time.AfterFunc(m.IdleTimeout, func() {
					m.mu.Lock()
					curSessions := m.sessions[serial]
					for i, s2 := range curSessions {
						if s2 == s && s2.refCount <= 0 {
							m.sessions[serial] = append(curSessions[:i], curSessions[i+1:]...)
							if len(m.sessions[serial]) == 0 {
								delete(m.sessions, serial)
							}
							m.mu.Unlock()
							s2.client.Close()
							return
						}
					}
					m.mu.Unlock()
				})
			}
			return
		}
	}
}

// Dial creates a new connection to the specified port on the device (via tunnel)
func (c *Client) Dial(port uint32) (net.Conn, error) {
	return c.tunnel.Dial(port)
}

// CloseAll closes all active sessions immediately.
func (m *SessionManager) CloseAll() {
	m.mu.Lock()
	var all []*managedSession
	for serial, sessions := range m.sessions {
		for _, s := range sessions {
			if s.idleTimer != nil {
				s.idleTimer.Stop()
			}
			all = append(all, s)
		}
		delete(m.sessions, serial)
	}
	m.mu.Unlock()

	for _, s := range all {
		s.client.Close()
	}
}
