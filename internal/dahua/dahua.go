package dahua

import (
	"context"
	"errors"
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
	"github.com/AlexxIT/go2rtc/pkg/rtsp"
	"github.com/blackjid/dahua-p2p"
	"github.com/rs/zerolog"
)

// negotiateLockTimeout bounds how long dahuaDial waits behind other streams
// for the per-client negotiate lock. Each negotiation is a BIND (up to 3 x 5s)
// plus a DESCRIBE (15s), so a full tunnel can legitimately queue for a while;
// past this we would rather fail and let the caller retry than hang a stream.
const negotiateLockTimeout = 90 * time.Second

// rtspCmdTimeout applies to RTSP command exchanges over the tunnel. The
// package default (5s) is tuned for LAN cameras and is too tight for an NVR
// answering DESCRIBE over a WAN UDP hole punch.
const rtspCmdTimeout = 15 * time.Second

var log zerolog.Logger
var sessions *dahua.SessionManager
var defaultMaxRealms int

// producer keeps Dahua-specific lifecycle and negotiation rules out of the
// shared RTSP package. The initial lock stays held through all SETUP requests;
// later GetTrack calls serialize any RTSP reconnect and its restored SETUPs.
type producer struct {
	*rtsp.Conn

	client        *dahua.Client
	serial        string
	negotiateMu   sync.Mutex
	negotiateHeld bool
	releaseOnce   sync.Once
}

func (p *producer) finishNegotiateLocked() {
	if !p.negotiateHeld {
		return
	}
	p.client.DoneNegotiate()
	p.client.UnlockNegotiate()
	p.negotiateHeld = false
}

func (p *producer) finishNegotiate() {
	p.negotiateMu.Lock()
	p.finishNegotiateLocked()
	p.negotiateMu.Unlock()
}

func (p *producer) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	p.negotiateMu.Lock()
	defer p.negotiateMu.Unlock()

	// The initial negotiation lock is acquired by dahuaDial before DESCRIBE
	// and remains held until Start. Once streaming, GetTrack may reconnect the
	// RTSP client, so each call takes the lock for its full Dial/DESCRIBE/SETUP.
	if !p.negotiateHeld {
		ctx, cancel := context.WithTimeout(context.Background(), negotiateLockTimeout)
		defer cancel()
		if err := p.client.LockNegotiate(ctx); err != nil {
			return nil, fmt.Errorf("negotiate lock: %w", err)
		}
		p.negotiateHeld = true
		p.client.WaitSettle()
		defer p.finishNegotiateLocked()
	}

	return p.Conn.GetTrack(media, codec)
}

func (p *producer) Start() error {
	p.finishNegotiate()
	return p.Conn.Start()
}

func (p *producer) Stop() error {
	p.finishNegotiate()
	err := p.Conn.Stop()
	p.releaseOnce.Do(func() { sessions.Release(p.serial, p.client) })
	return err
}

func Init() {
	var cfg struct {
		Mod struct {
			MaxRealms int `yaml:"max_realms"`
		} `yaml:"dahua"`
	}
	app.LoadConfig(&cfg)

	log = app.GetLogger("dahua")
	sessions = dahua.NewSessionManager()
	defaultMaxRealms = cfg.Mod.MaxRealms

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

	var p2pPort int
	if s := query.Get("p2p_port"); s != "" {
		if p2pPort, err = strconv.Atoi(s); err != nil || p2pPort < 1 || p2pPort > 65535 {
			return nil, fmt.Errorf("invalid p2p_port %q", s)
		}
	}

	maxRealms := defaultMaxRealms
	if s := query.Get("max_realms"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("invalid max_realms %q", s)
		}
		maxRealms = n
	}

	log.Info().Str("serial", serial).Str("channel", channel).Str("subtype", subtype).
		Int("p2p_port", p2pPort).Int("max_realms", maxRealms).Msg("[dahua] connecting via P2P")

	// Each attempt already burns its own internal retries (3 BINDs, or 3
	// direct-handshake attempts); these outer attempts exist to cover a
	// tunnel that died and has to be rebuilt from scratch. 2s is enough
	// breathing room for the device without exceeding HA's stream timeout.
	const maxAttempts = 4
	const backoff = 2 * time.Second

	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		conn, err := dahuaDial(serial, user, pass, channel, subtype, p2pPort, maxRealms)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if i == maxAttempts-1 {
			break
		}
		// A retired tunnel is not a failure: the next Acquire simply builds a
		// fresh one, so there is nothing to back off from.
		if errors.Is(err, dahua.ErrTunnelRetired) {
			log.Debug().Int("attempt", i+1).Str("serial", serial).
				Msg("[dahua] tunnel retired, retrying on a fresh tunnel")
			continue
		}
		log.Warn().Err(err).Int("attempt", i+1).Int("max", maxAttempts).
			Str("serial", serial).Msg("[dahua] connection failed, retrying")
		time.Sleep(backoff)
	}
	log.Error().Err(lastErr).Str("serial", serial).Msg("[dahua] all connection attempts failed")
	return nil, lastErr
}

func dahuaDial(serial, user, pass, channel, subtype string, p2pPort, maxRealms int) (core.Producer, error) {
	client, err := sessions.Acquire(dahua.Config{
		Serial:    serial,
		Username:  user,
		Password:  pass,
		P2PPort:   p2pPort,
		MaxRealms: maxRealms,
		// pkg/dahua owns no logger; wire its hooks to ours.
		Trace: func(format string, v ...any) { log.Debug().Msgf("[dahua] "+format, v...) },
		Error: func(format string, v ...any) { log.Warn().Msgf("[dahua] "+format, v...) },
	})
	if err != nil {
		return nil, fmt.Errorf("P2P handshake failed: %w", err)
	}

	// Serialize RTSP negotiation: the device can't reliably handle
	// concurrent OPTIONS/DESCRIBE/SETUP through the PTCP tunnel.
	ctx, cancel := context.WithTimeout(context.Background(), negotiateLockTimeout)
	defer cancel()
	if err := client.LockNegotiate(ctx); err != nil {
		sessions.Release(serial, client)
		return nil, fmt.Errorf("negotiate lock: %w", err)
	}

	client.WaitSettle()
	finishNegotiate := func() {
		client.DoneNegotiate()
		client.UnlockNegotiate()
	}

	// dialRealm opens a realm on the shared tunnel. rtsp calls it on every
	// Dial, so a Reconnect (a second consumer asking for a media after PLAY)
	// gets a fresh realm instead of the dead one it just closed.
	dialRealm := func() (net.Conn, error) {
		return client.Dial(client.RTSPPort())
	}

	rtspURL := (&url.URL{
		Scheme:   "rtsp",
		User:     url.UserPassword(user, pass),
		Host:     "tunnel", // placeholder: the dialer decides the transport
		Path:     "/cam/realmonitor",
		RawQuery: url.Values{"channel": {channel}, "subtype": {subtype}}.Encode(),
	}).String()

	log.Debug().Str("serial", serial).Str("channel", channel).Str("subtype", subtype).
		Msg("[dahua] starting RTSP over P2P tunnel")

	conn := rtsp.NewClientWithDialer(rtspURL, dialRealm)
	conn.UserAgent = app.UserAgent
	conn.Timeout = 15
	conn.CmdTimeout = rtspCmdTimeout

	if err := conn.Dial(); err != nil {
		finishNegotiate()
		sessions.Release(serial, client)
		return nil, fmt.Errorf("tunnel dial failed: %w", err)
	}

	describeStart := time.Now()
	if err := conn.Describe(); err != nil {
		log.Debug().Err(err).Str("serial", serial).Dur("elapsed", time.Since(describeStart)).
			Msg("[dahua] RTSP DESCRIBE failed")
		_ = conn.Close()
		finishNegotiate()
		sessions.Release(serial, client)
		return nil, fmt.Errorf("RTSP describe failed: %w", err)
	}
	log.Debug().Str("serial", serial).Dur("elapsed", time.Since(describeStart)).
		Msg("[dahua] RTSP DESCRIBE ok")

	// RTSP closes the realm on reconnect and final shutdown. The wrapper owns
	// the manager reservation until final Stop, so reconnect cannot briefly
	// expose the slot to another producer.
	conn.OnClose = func() error {
		log.Debug().Str("serial", serial).Msg("[dahua] RTSP realm closed")
		return nil
	}

	return &producer{
		Conn:          conn,
		client:        client,
		serial:        serial,
		negotiateHeld: true,
	}, nil
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

	numChannels := 1
	if s := query.Get("channels"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > 64 {
			http.Error(w, "invalid channels parameter", http.StatusBadRequest)
			return
		}
		numChannels = n
	}

	// Credentials go through url.UserPassword so a password containing
	// @ : / ? or # still yields a URL that parses back to the same value.
	userinfo := url.UserPassword(user, pass).String()

	var items []*api.Source
	for ch := 1; ch <= numChannels; ch++ {
		for _, st := range []struct {
			name    string
			subtype int
		}{{"Main Stream", 0}, {"Sub Stream", 1}} {
			items = append(items, &api.Source{
				Name: fmt.Sprintf("Channel %d - %s", ch, st.name),
				Info: fmt.Sprintf("serial=%s channel=%d subtype=%d", serial, ch, st.subtype),
				URL: fmt.Sprintf("dahua://%s@%s?channel=%d&subtype=%d",
					userinfo, url.QueryEscape(serial), ch, st.subtype),
			})
		}
	}

	api.ResponseSources(w, items)
}
