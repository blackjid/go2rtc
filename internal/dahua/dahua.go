package dahua

import (
	"fmt"
	"net"
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

var log zerolog.Logger
var sessions *dahua.SessionManager

func Init() {
	log = app.GetLogger("dahua")
	sessions = dahua.NewSessionManager()

	streams.HandleFunc("dahua", dahuaHandler)

	api.HandleFunc("api/dahua", apiDahua)
}

// Shutdown closes all active P2P sessions, notifying devices of disconnection.
// Call this on process exit so devices can immediately free the P2P session
// instead of waiting for heartbeat timeout.
func Shutdown() {
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

	relayMode := query.Get("relay") == "true"

	var p2pPort int
	if portStr := query.Get("p2p_port"); portStr != "" {
		p2pPort, _ = strconv.Atoi(portStr)
	}

	log.Info().Str("serial", serial).Str("channel", channel).Str("subtype", subtype).Bool("relay", relayMode).Int("p2p_port", p2pPort).Msg("[dahua] connecting via P2P")

	type attempt struct {
		relay   bool
		backoff time.Duration
	}

	attempts := []attempt{
		{relay: relayMode, backoff: dahua.HandshakeCooldown + 5*time.Second},
		{relay: relayMode, backoff: dahua.HandshakeCooldown + 5*time.Second},
	}
	if !relayMode {
		// After direct mode fails twice, fall back to relay
		attempts = append(attempts,
			attempt{relay: true, backoff: dahua.HandshakeCooldown + 5*time.Second},
			attempt{relay: true},
		)
	}

	var lastErr error
	for i, a := range attempts {
		conn, err := dahuaDial(serial, user, pass, channel, subtype, a.relay, p2pPort)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if i < len(attempts)-1 {
			log.Warn().Err(err).Int("attempt", i+1).Int("max", len(attempts)).Bool("relay", a.relay).Dur("backoff", a.backoff).Str("serial", serial).Msg("[dahua] connection failed, retrying")
			time.Sleep(a.backoff)
		}
	}
	log.Error().Err(lastErr).Str("serial", serial).Msg("[dahua] all connection attempts failed")
	return nil, lastErr
}

func dahuaDial(serial, user, pass, channel, subtype string, relayMode bool, p2pPort int) (core.Producer, error) {
	client, err := sessions.Acquire(dahua.Config{
		Serial:    serial,
		Username:  user,
		Password:  pass,
		RelayMode: relayMode,
		P2PPort:   p2pPort,
	})
	if err != nil {
		return nil, fmt.Errorf("P2P handshake failed: %w", err)
	}

	listener, err := client.Listen("127.0.0.1:0", 554)
	if err != nil {
		sessions.Invalidate(serial)
		return nil, fmt.Errorf("listen failed: %w", err)
	}

	localPort := listener.Addr().(*net.TCPAddr).Port
	rtspURL := fmt.Sprintf("rtsp://%s:%s@127.0.0.1:%d/cam/realmonitor?channel=%s&subtype=%s",
		user, pass, localPort, channel, subtype)

	log.Debug().Str("rtsp_url", rtspURL).Msg("[dahua] starting RTSP via local bridge")

	conn := rtsp.NewClient(rtspURL)
	conn.UserAgent = app.UserAgent

	if err := conn.Dial(); err != nil {
		listener.Close()
		sessions.Invalidate(serial)
		return nil, fmt.Errorf("RTSP dial failed: %w", err)
	}

	if err := conn.Describe(); err != nil {
		listener.Close()
		sessions.Invalidate(serial)
		return nil, fmt.Errorf("RTSP describe failed: %w", err)
	}

	var cleanupOnce sync.Once
	conn.OnClose = func() error {
		cleanupOnce.Do(func() {
			log.Debug().Str("serial", serial).Msg("[dahua] RTSP connection closed, cleaning up")
			listener.Close()
			sessions.Release(serial)
		})
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
