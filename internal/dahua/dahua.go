package dahua

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/internal/api"
	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/dahua"
	"github.com/AlexxIT/go2rtc/pkg/rtsp"
	"github.com/rs/zerolog"
)

const streamLingerDuration = 30 * time.Second

var log zerolog.Logger
var sessions *dahua.SessionManager
var lingerMu sync.Mutex
var lingerTimers = map[string]*time.Timer{} // key: serial, delays Release on disconnect

func Init() {
	log = app.GetLogger("dahua")
	sessions = dahua.NewSessionManager()

	streams.HandleFunc("dahua", dahuaHandler)

	api.HandleFunc("api/dahua", apiDahua)
}

// cancelLinger cancels a pending linger timer for the given serial, if any.
func cancelLinger(serial string) {
	lingerMu.Lock()
	if t, ok := lingerTimers[serial]; ok {
		t.Stop()
		delete(lingerTimers, serial)
	}
	lingerMu.Unlock()
}

// Shutdown closes all active P2P sessions, notifying devices of disconnection.
// Call this on process exit so devices can immediately free the P2P session
// instead of waiting for heartbeat timeout.
func Shutdown() {
	lingerMu.Lock()
	for serial, t := range lingerTimers {
		t.Stop()
		delete(lingerTimers, serial)
	}
	lingerMu.Unlock()

	if sessions != nil {
		log.Debug().Msg("[dahua] shutting down P2P sessions")
		sessions.CloseAll()
	}
}

func dahuaHandler(rawURL string) (core.Producer, error) {
	// Parse dahua://user:pass@SERIAL?channel=1&subtype=0
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid dahua URL: %w", err)
	}

	serial := u.Hostname()
	if serial == "" {
		return nil, fmt.Errorf("missing serial number in dahua URL")
	}

	var user, pass string
	if u.User != nil {
		user = u.User.Username()
		pass, _ = u.User.Password()
	}

	query := u.Query()
	channel := query.Get("channel")
	if channel == "" {
		channel = "1"
	}
	subtype := query.Get("subtype")
	if subtype == "" {
		subtype = "0"
	}

	var p2pPort int
	if portStr := query.Get("p2p_port"); portStr != "" {
		p2pPort, _ = strconv.Atoi(portStr)
	}

	log.Info().Str("serial", serial).Str("channel", channel).Str("subtype", subtype).Int("p2p_port", p2pPort).Msg("[dahua] connecting via P2P")

	const maxAttempts = 4
	backoff := dahua.HandshakeCooldown + 5*time.Second

	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		conn, err := dahuaDial(serial, user, pass, channel, subtype, p2pPort)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if i < maxAttempts-1 {
			log.Warn().Err(err).Int("attempt", i+1).Int("max", maxAttempts).Dur("backoff", backoff).Str("serial", serial).Msg("[dahua] connection failed, retrying")
			time.Sleep(backoff)
		}
	}
	log.Error().Err(lastErr).Str("serial", serial).Msg("[dahua] all connection attempts failed")
	return nil, lastErr
}

func dahuaDial(serial, user, pass, channel, subtype string, p2pPort int) (core.Producer, error) {
	cancelLinger(serial)

	client, err := sessions.Acquire(dahua.Config{
		Serial:   serial,
		Username: user,
		Password: pass,
		P2PPort:  p2pPort,
	})
	if err != nil {
		return nil, fmt.Errorf("P2P handshake failed: %w", err)
	}

	// Serialize RTSP negotiation: the device can't reliably handle
	// concurrent OPTIONS/DESCRIBE/SETUP through the PTCP tunnel.
	client.NegotiateMu.Lock()
	client.WaitSettle()

	tunnelConn, err := client.Dial(554)
	if err != nil {
		client.DoneNegotiate()
		client.NegotiateMu.Unlock()
		if client.IsClosed() {
			sessions.Invalidate(serial)
		} else {
			sessions.Release(serial, client)
		}
		return nil, fmt.Errorf("tunnel dial failed: %w", err)
	}

	rtspURL := fmt.Sprintf("rtsp://%s:%s@tunnel/cam/realmonitor?channel=%s&subtype=%s",
		user, pass, channel, subtype)

	log.Debug().Str("rtsp_url", rtspURL).Msg("[dahua] starting RTSP via in-memory bridge")

	conn := rtsp.NewClientWithConn(rtspURL, tunnelConn)
	conn.UserAgent = app.UserAgent
	conn.Timeout = 15
	conn.CmdTimeout = 15 * time.Second

	if err := conn.Dial(); err != nil {
		tunnelConn.Close()
		client.NegotiateMu.Unlock()
		if client.IsClosed() {
			sessions.Invalidate(serial)
		} else {
			sessions.Release(serial, client)
		}
		return nil, fmt.Errorf("RTSP dial failed: %w", err)
	}

	if err := conn.Describe(); err != nil {
		tunnelConn.Close()
		client.NegotiateMu.Unlock()
		if client.IsClosed() {
			sessions.Invalidate(serial)
		} else {
			sessions.Release(serial, client)
		}
		return nil, fmt.Errorf("RTSP describe failed: %w", err)
	}

	client.DoneNegotiate()
	client.NegotiateMu.Unlock()

	var cleanupOnce sync.Once
	doCleanup := func() {
		cleanupOnce.Do(func() {
			log.Debug().Str("serial", serial).Msg("[dahua] releasing P2P session")
			tunnelConn.Close()
			sessions.Release(serial, client)
		})
	}
	conn.OnClose = func() error {
		log.Debug().Str("serial", serial).Dur("linger", streamLingerDuration).Msg("[dahua] RTSP closed, starting linger")
		lingerMu.Lock()
		if t, ok := lingerTimers[serial]; ok {
			t.Stop()
		}
		lingerTimers[serial] = time.AfterFunc(streamLingerDuration, func() {
			lingerMu.Lock()
			delete(lingerTimers, serial)
			lingerMu.Unlock()
			doCleanup()
		})
		lingerMu.Unlock()
		return nil
	}

	return conn, nil
}

func apiDahua(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	serial := query.Get("serial")
	if serial == "" {
		http.Error(w, "serial parameter required", http.StatusBadRequest)
		return
	}

	user := query.Get("username")
	pass := query.Get("password")
	channels := query.Get("channels")
	if channels == "" {
		channels = "1"
	}

	numChannels := 1
	fmt.Sscanf(channels, "%d", &numChannels)
	if numChannels < 1 {
		numChannels = 1
	}

	var items []*api.Source
	for ch := 1; ch <= numChannels; ch++ {
		// Main stream (subtype=0)
		items = append(items, &api.Source{
			Name: fmt.Sprintf("Channel %d - Main Stream", ch),
			Info: fmt.Sprintf("serial=%s channel=%d subtype=0", serial, ch),
			URL:  fmt.Sprintf("dahua://%s:%s@%s?channel=%d&subtype=0", user, pass, serial, ch),
		})
		// Sub stream (subtype=1)
		items = append(items, &api.Source{
			Name: fmt.Sprintf("Channel %d - Sub Stream", ch),
			Info: fmt.Sprintf("serial=%s channel=%d subtype=1", serial, ch),
			URL:  fmt.Sprintf("dahua://%s:%s@%s?channel=%d&subtype=1", user, pass, serial, ch),
		})
	}

	api.ResponseSources(w, items)
}
