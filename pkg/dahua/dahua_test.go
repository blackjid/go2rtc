package dahua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/dahua/tunnel"
)

// liveTunnel is a tunnel the device is currently answering. findAvailableSession
// skips silent ones, so a fake that never acked would never be handed out.
func liveTunnel() *tunnel.Tunnel {
	t := &tunnel.Tunnel{}
	t.MarkResponsive()
	return t
}

func TestFindAvailableSessionUsesReservations(t *testing.T) {
	key := sessionKeyFor(Config{Serial: "device", MaxRealms: 2})
	client := &Client{tunnel: liveTunnel()}
	session := &managedSession{client: client, refCount: 2, maxRealms: 2}
	manager := NewSessionManager()
	manager.sessions[key] = []*managedSession{session}

	if got := manager.findAvailableSession(key); got != nil {
		t.Fatal("full session was returned before its reserved realms were dialed")
	}

	session.refCount--
	if got := manager.findAvailableSession(key); got != session {
		t.Fatal("session with a free reservation was not returned")
	}
}

func TestClosedSessionManagerRejectsAcquire(t *testing.T) {
	manager := NewSessionManager()
	manager.CloseAll()

	if _, err := manager.Acquire(Config{}); !errors.Is(err, ErrSessionManagerClosed) {
		t.Fatalf("Acquire after CloseAll = %v, want ErrSessionManagerClosed", err)
	}
}

func TestFixedPortAtCapacityDoesNotOpenSecondTunnel(t *testing.T) {
	cfg := Config{Serial: "device", P2PPort: 5000, MaxRealms: 1}
	key := sessionKeyFor(cfg)
	manager := NewSessionManager()
	manager.sessions[key] = []*managedSession{{
		client:    &Client{tunnel: liveTunnel()},
		refCount:  1,
		maxRealms: 1,
	}}

	if _, err := manager.Acquire(cfg); !errors.Is(err, ErrFixedPortCapacity) {
		t.Fatalf("Acquire at fixed-port capacity = %v, want ErrFixedPortCapacity", err)
	}
}

func TestSessionKeyIncludesTunnelConfiguration(t *testing.T) {
	base := Config{Serial: "device", Username: "user", Password: "pass", P2PPort: 5000, MaxRealms: 4}
	want := sessionKeyFor(base)

	tests := []Config{
		{Serial: "other", Username: "user", Password: "pass", P2PPort: 5000, MaxRealms: 4},
		{Serial: "device", Username: "other", Password: "pass", P2PPort: 5000, MaxRealms: 4},
		{Serial: "device", Username: "user", Password: "other", P2PPort: 5000, MaxRealms: 4},
		{Serial: "device", Username: "user", Password: "pass", P2PPort: 5001, MaxRealms: 4},
		{Serial: "device", Username: "user", Password: "pass", P2PPort: 5000, MaxRealms: 5},
	}
	for _, cfg := range tests {
		if got := sessionKeyFor(cfg); got == want {
			t.Fatalf("configuration did not change session key: %+v", cfg)
		}
	}
}

// A stream queued for the negotiate lock has an RTSP client counting against
// it. Once the tunnel retires, its turn will only bring a refusal, so waiting
// it out spends the client's whole budget to learn nothing.
func TestLockNegotiateGivesUpOnRetiredTunnel(t *testing.T) {
	client := &Client{tunnel: &tunnel.Tunnel{}, negotiateSem: make(chan struct{}, 1)}
	if err := client.LockNegotiate(context.Background()); err != nil {
		t.Fatalf("uncontended lock: %v", err)
	}

	queued := make(chan error, 1)
	go func() { queued <- client.LockNegotiate(context.Background()) }()

	client.Retire()

	select {
	case err := <-queued:
		if !errors.Is(err, ErrTunnelRetired) {
			t.Fatalf("queued lock = %v, want ErrTunnelRetired", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued caller kept waiting for a tunnel that had retired")
	}
}

// A fixed port admits one tunnel. One that is retired or silent can take no
// new stream, so it must make way instead of blocking every Acquire until it
// dies on its own. A healthy one, even full, stays.
func TestFixedPortEvictsOnlyUnusableTunnels(t *testing.T) {
	cfg := Config{Serial: "device", P2PPort: 5000, MaxRealms: 1}
	key := sessionKeyFor(cfg)

	retired := &Client{tunnel: liveTunnel()}
	retired.Retire()
	silent := &Client{tunnel: &tunnel.Tunnel{}}
	healthy := &Client{tunnel: liveTunnel()}

	manager := NewSessionManager()
	manager.sessions[key] = []*managedSession{
		{client: retired, refCount: 1, maxRealms: 1},
		{client: silent, refCount: 0, maxRealms: 1},
		{client: healthy, refCount: 1, maxRealms: 1},
	}

	stale := manager.evictUnusable(key)
	if len(stale) != 2 || stale[0].client != retired || stale[1].client != silent {
		t.Fatalf("evicted %d sessions, want the retired and the silent one", len(stale))
	}
	if got := manager.sessions[key]; len(got) != 1 || got[0].client != healthy {
		t.Fatalf("healthy tunnel was not kept")
	}
	if _, err := manager.Acquire(cfg); !errors.Is(err, ErrFixedPortCapacity) {
		t.Fatalf("Acquire with a full healthy tunnel = %v, want ErrFixedPortCapacity", err)
	}
}

func TestSettleIsPacedPerDeviceNotPerTunnel(t *testing.T) {
	const serial = "paced-device"
	negotiateClocks.Delete(serial)

	settle := 40 * time.Millisecond
	first := &Client{serial: serial, settle: settle}
	second := &Client{serial: serial, settle: settle}

	// A different tunnel to the same device must queue behind the first
	// one's negotiation: the device stalls on realm setup rate, and it
	// stalls device-wide.
	first.DoneNegotiate()

	start := time.Now()
	second.WaitSettle()

	if waited := time.Since(start); waited < settle/2 {
		t.Fatalf("second tunnel waited %v, want at least %v", waited, settle/2)
	}
}

func TestSettleDoesNotDelayTheFirstNegotiation(t *testing.T) {
	const serial = "fresh-device"
	negotiateClocks.Delete(serial)

	client := &Client{serial: serial, settle: time.Second}

	start := time.Now()
	client.WaitSettle()

	if waited := time.Since(start); waited > 100*time.Millisecond {
		t.Fatalf("first negotiation waited %v, want no wait", waited)
	}
}

func TestSettleIntervalIsConfigurable(t *testing.T) {
	cfg := Config{Serial: "device", NegotiateSettle: 3 * time.Second}
	if got := settleFor(cfg); got != 3*time.Second {
		t.Fatalf("settleFor(%v) = %v, want 3s", cfg.NegotiateSettle, got)
	}
	if got := settleFor(Config{Serial: "device"}); got != NegotiateSettle {
		t.Fatalf("settleFor(0) = %v, want the default %v", got, NegotiateSettle)
	}
}

func TestSilentTunnelIsNotHandedToNewStreams(t *testing.T) {
	key := sessionKeyFor(Config{Serial: "device", MaxRealms: 4})

	// A tunnel the device has stopped answering. It is neither closed nor
	// retired -- the heartbeat timeout allows two minutes before either --
	// and it still has room, which is exactly how a dead tunnel collects
	// streams that each spend a full BIND budget discovering it is dead.
	silent := &managedSession{client: &Client{tunnel: &tunnel.Tunnel{}}, refCount: 1, maxRealms: 4}

	manager := NewSessionManager()
	manager.sessions[key] = []*managedSession{silent}

	if got := manager.findAvailableSession(key); got != nil {
		t.Fatal("a tunnel the device has gone silent on was handed to a new stream")
	}

	// It comes back into rotation the moment the device answers again.
	silent.client.tunnel.MarkResponsive()
	if got := manager.findAvailableSession(key); got != silent {
		t.Fatal("a responsive tunnel with room was not handed out")
	}
}

func TestSilentTunnelKeepsTheStreamsItHas(t *testing.T) {
	key := sessionKeyFor(Config{Serial: "device", MaxRealms: 4})
	silent := &managedSession{client: &Client{tunnel: &tunnel.Tunnel{}}, refCount: 2, maxRealms: 4}

	manager := NewSessionManager()
	manager.sessions[key] = []*managedSession{silent}
	manager.findAvailableSession(key)

	// Skipped, not evicted: the tunnel may yet come back, and the streams on
	// it have nowhere better to be.
	if got := manager.sessions[key]; len(got) != 1 || got[0] != silent {
		t.Fatalf("silent session was dropped from the pool: %v", got)
	}
}
