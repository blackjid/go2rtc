package dh

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/dahua/ptcp"
	"github.com/rs/zerolog/log"
)

// HandshakeResult contains the result of a successful P2P handshake
type HandshakeResult struct {
	Client     *UDPClient
	Session    *ptcp.Session
	DeviceIP   string
	DevicePort int
}

// HandshakeOptions contains options for the P2P handshake
type HandshakeOptions struct {
	Serial  string
	Timeout time.Duration

	// Device authentication credentials (required for devices with per-device randsalt)
	DeviceUsername string
	DevicePassword string

	// P2PPort is the local UDP port for the device connection.
	// Use a fixed port with k8s hostPort to bypass pod-level NAT.
	// 0 means random ephemeral port (default).
	P2PPort int
}

// Handshake performs the full P2P handshake sequence
func Handshake(opts HandshakeOptions) (*HandshakeResult, error) {
	if opts.Timeout == 0 {
		opts.Timeout = DefaultTimeout
	}

	// Step 1: Connect to main server and get P2P server info
	mainClient, err := NewUDPClient(opts.Timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to create main client: %w", err)
	}

	if err := mainClient.Connect(MainServer, MainPort); err != nil {
		mainClient.Close()
		return nil, fmt.Errorf("failed to connect to main server: %w", err)
	}

	log.Debug().Str("server", fmt.Sprintf("%s:%d", MainServer, MainPort)).Msg("[dahua] connected to main server")

	// Probe P2P server
	if err := mainClient.Request("/probe/p2psrv", ""); err != nil {
		mainClient.Close()
		return nil, fmt.Errorf("failed to probe p2psrv: %w", err)
	}
	if _, err := mainClient.Read(); err != nil {
		mainClient.Close()
		return nil, fmt.Errorf("failed to read probe response: %w", err)
	}

	// Get P2P server for device
	if err := mainClient.Request(fmt.Sprintf("/online/p2psrv/%s", opts.Serial), ""); err != nil {
		mainClient.Close()
		return nil, fmt.Errorf("failed to get p2psrv: %w", err)
	}
	p2pResp, err := mainClient.Read()
	if err != nil {
		mainClient.Close()
		return nil, fmt.Errorf("failed to read p2psrv response: %w", err)
	}

	p2psrv, ok := p2pResp.Body["body/US"]
	if !ok {
		mainClient.Close()
		return nil, ErrDeviceNotFound
	}

	log.Debug().Str("server", p2psrv).Msg("[dahua] P2P server")

	// Step 2: Connect to P2P server and probe device
	p2pParts := strings.Split(p2psrv, ":")
	if len(p2pParts) != 2 {
		mainClient.Close()
		return nil, ErrInvalidResponse
	}
	p2pPort, _ := strconv.Atoi(p2pParts[1])

	p2pClient, err := NewUDPClient(opts.Timeout)
	if err != nil {
		mainClient.Close()
		return nil, fmt.Errorf("failed to create p2p client: %w", err)
	}

	if err := p2pClient.Connect(p2pParts[0], p2pPort); err != nil {
		mainClient.Close()
		p2pClient.Close()
		return nil, fmt.Errorf("failed to connect to p2p server: %w", err)
	}

	// Probe device
	if err := p2pClient.Request(fmt.Sprintf("/probe/device/%s", opts.Serial), ""); err != nil {
		mainClient.Close()
		p2pClient.Close()
		return nil, fmt.Errorf("failed to probe device: %w", err)
	}
	if _, err := p2pClient.Read(); err != nil {
		mainClient.Close()
		p2pClient.Close()
		return nil, fmt.Errorf("failed to read device probe response: %w", err)
	}

	// Query device info to get randsalt
	if err := p2pClient.Request(fmt.Sprintf("/info/device/%s", opts.Serial), ""); err != nil {
		mainClient.Close()
		p2pClient.Close()
		return nil, fmt.Errorf("failed to request device info: %w", err)
	}
	infoResp, err := p2pClient.Read()
	if err != nil {
		mainClient.Close()
		p2pClient.Close()
		return nil, fmt.Errorf("failed to read device info response: %w", err)
	}

	var randsalt string
	if infoField, ok := infoResp.Body["body/Info"]; ok && infoField != "" {
		deviceInfo, err := DecryptDeviceInfo(infoField)
		if err != nil {
			log.Debug().Err(err).Msg("[dahua] could not decrypt device info")
		} else {
			randsalt = deviceInfo.RandSalt
			log.Debug().Str("randsalt", randsalt).Int("rtspport", deviceInfo.RTSPPort).Msg("[dahua] device info")
		}
	}

	// Step 3: Get relay server info
	if err := mainClient.Request("/online/relay", ""); err != nil {
		mainClient.Close()
		p2pClient.Close()
		return nil, fmt.Errorf("failed to get relay: %w", err)
	}
	relayResp, err := mainClient.Read()
	if err != nil {
		mainClient.Close()
		p2pClient.Close()
		return nil, fmt.Errorf("failed to read relay response: %w", err)
	}

	relayAddr, ok := relayResp.Body["body/Address"]
	if !ok {
		mainClient.Close()
		p2pClient.Close()
		return nil, ErrInvalidResponse
	}

	log.Debug().Str("relay", relayAddr).Msg("[dahua] relay server")

	// Step 4: Create device connection client and request P2P channel
	log.Debug().Msg("[dahua] step 4: requesting P2P channel from device")
	deviceClient, err := NewUDPClientPort(opts.Timeout, opts.P2PPort)
	if err != nil {
		mainClient.Close()
		p2pClient.Close()
		return nil, fmt.Errorf("failed to create device client: %w", err)
	}

	if err := deviceClient.Connect(MainServer, MainPort); err != nil {
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to connect device client: %w", err)
	}

	// Generate identify
	cid := RandomBytes(8)
	localAddr := fmt.Sprintf("127.0.0.1:%d", deviceClient.LocalPort())

	var deviceKey []byte
	var deviceNonce int

	needsAuth := opts.DeviceUsername != "" && opts.DevicePassword != "" && randsalt != ""

	if needsAuth {
		deviceKey = GetDeviceKey(opts.DeviceUsername, opts.DevicePassword, randsalt)
		deviceNonce = GetDeviceNonce()

		encAddr, err := EncryptAddr(deviceKey, deviceNonce, localAddr)
		if err != nil {
			mainClient.Close()
			deviceClient.Close()
			return nil, fmt.Errorf("failed to encrypt address: %w", err)
		}

		authXML := GetDeviceAuth(opts.DeviceUsername, deviceKey, deviceNonce, randsalt, encAddr)
		body := fmt.Sprintf("<body>%s<Identify>%s</Identify><IpEncrptV2>true</IpEncrptV2><LocalAddr>%s</LocalAddr><version>5.0.0</version></body>",
			authXML, formatIdentify(cid), encAddr)

		if err := deviceClient.Request(fmt.Sprintf("/device/%s/p2p-channel", opts.Serial), body); err != nil {
			mainClient.Close()
			deviceClient.Close()
			return nil, fmt.Errorf("failed to request p2p channel: %w", err)
		}
	} else {
		body := fmt.Sprintf("<body><Identify>%s</Identify><IpEncrpt>true</IpEncrpt><LocalAddr>%s</LocalAddr><version>5.0.0</version></body>",
			formatIdentify(cid), localAddr)

		if err := deviceClient.Request(fmt.Sprintf("/device/%s/p2p-channel", opts.Serial), body); err != nil {
			mainClient.Close()
			deviceClient.Close()
			return nil, fmt.Errorf("failed to request p2p channel: %w", err)
		}
	}

	// Step 5: Connect to relay and get agent info
	log.Debug().Str("relay", relayAddr).Msg("[dahua] step 5: connecting to relay for agent info")
	relayParts := strings.Split(relayAddr, ":")
	if len(relayParts) != 2 {
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, ErrInvalidResponse
	}
	relayPort, _ := strconv.Atoi(relayParts[1])

	if err := p2pClient.Connect(relayParts[0], relayPort); err != nil {
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to connect to relay: %w", err)
	}

	if err := p2pClient.Request("/relay/agent", ""); err != nil {
		log.Error().Err(err).Msg("[dahua] failed to request relay agent")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to request relay agent: %w", err)
	}
	agentResp, err := p2pClient.Read()
	if err != nil {
		log.Error().Err(err).Msg("[dahua] failed to read agent response (timeout?)")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to read agent response: %w", err)
	}

	token, ok := agentResp.Body["body/Token"]
	if !ok {
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, ErrInvalidResponse
	}
	agentAddr, ok := agentResp.Body["body/Agent"]
	if !ok {
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, ErrInvalidResponse
	}

	log.Debug().Str("agent", agentAddr).Str("token", token).Msg("[dahua] agent info")

	// Step 6: Connect to agent and start relay
	log.Debug().Str("agent", agentAddr).Msg("[dahua] step 6: connecting to agent")
	agentParts := strings.Split(agentAddr, ":")
	if len(agentParts) != 2 {
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, ErrInvalidResponse
	}
	agentPort, _ := strconv.Atoi(agentParts[1])

	if err := p2pClient.Connect(agentParts[0], agentPort); err != nil {
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to connect to agent: %w", err)
	}

	log.Debug().Str("token", token).Msg("[dahua] step 6: starting relay")
	if err := p2pClient.Request(fmt.Sprintf("/relay/start/%s", token), "<body><Client>:0</Client></body>"); err != nil {
		log.Error().Err(err).Msg("[dahua] failed to start relay")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to start relay: %w", err)
	}
	if _, err := p2pClient.Read(); err != nil {
		log.Error().Err(err).Msg("[dahua] failed to read relay start response")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to read relay start response: %w", err)
	}

	// Step 7: Read P2P channel response from device
	log.Debug().Msg("[dahua] step 7: reading P2P channel response from device")
	channelResp, err := deviceClient.Read()
	if err != nil {
		log.Error().Err(err).Msg("[dahua] failed to read P2P channel response from device")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to read p2p channel response: %w", err)
	}

	// Handle 100 Continue
	if channelResp.Code == 100 {
		channelResp, err = deviceClient.Read()
		if err != nil {
			mainClient.Close()
			p2pClient.Close()
			deviceClient.Close()
			return nil, fmt.Errorf("failed to read final p2p channel response: %w", err)
		}
	}

	if channelResp.Code >= 400 {
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		if channelResp.Code == 403 {
			return nil, ErrAuthenticationFailed
		}
		return nil, fmt.Errorf("p2p channel error: %s", channelResp.Status)
	}

	deviceLAddr, ok := channelResp.Body["body/LocalAddr"]
	if !ok {
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, ErrInvalidResponse
	}

	// Decrypt local address if using authenticated mode
	if needsAuth {
		respNonceStr, _ := channelResp.Body["body/Nonce"]
		if respNonceStr != "" {
			respNonce, _ := strconv.Atoi(respNonceStr)
			decAddr, err := DecryptAddr(deviceKey, respNonce, deviceLAddr)
			if err == nil {
				deviceLAddr = decAddr
			}
		}
	}

	devicePubAddr, ok := channelResp.Body["body/PubAddr"]
	if !ok {
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, ErrInvalidResponse
	}

	log.Debug().Str("pub", devicePubAddr).Str("local", deviceLAddr).Msg("[dahua] device address")

	deviceParts := strings.Split(devicePubAddr, ":")
	if len(deviceParts) != 2 {
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("invalid device address format: %s", devicePubAddr)
	}
	devicePort, _ := strconv.Atoi(deviceParts[1])

	// Connect deviceClient to device public address
	log.Debug().Str("addr", devicePubAddr).Msg("[dahua] step 8: connecting to device public address")
	if err := deviceClient.Connect(deviceParts[0], devicePort); err != nil {
		log.Error().Err(err).Str("addr", devicePubAddr).Msg("[dahua] failed to connect to device")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to connect to device: %w", err)
	}

	// Step 8: Request relay channel
	log.Debug().Msg("[dahua] step 9: requesting relay channel")
	if err := p2pClient.Connect(MainServer, MainPort); err != nil {
		log.Error().Err(err).Msg("[dahua] failed to reconnect to main server for relay channel")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to reconnect to main server: %w", err)
	}

	var relayChannelBody string
	if needsAuth {
		respNonceStr, _ := channelResp.Body["body/Nonce"]
		respNonce, _ := strconv.Atoi(respNonceStr)
		relayAuth := GetDeviceAuth(opts.DeviceUsername, deviceKey, respNonce, randsalt, "")
		relayChannelBody = fmt.Sprintf("<body>%s<agentAddr>%s</agentAddr></body>", relayAuth, agentAddr)
	} else {
		relayChannelBody = fmt.Sprintf("<body><agentAddr>%s</agentAddr></body>", agentAddr)
	}
	if err := p2pClient.Request(fmt.Sprintf("/device/%s/relay-channel", opts.Serial), relayChannelBody); err != nil {
		log.Error().Err(err).Msg("[dahua] failed to request relay channel")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to request relay channel: %w", err)
	}

	// Step 9: Connect to agent for PTCP session
	log.Debug().Str("agent", agentAddr).Msg("[dahua] step 10: connecting to agent for PTCP")
	if err := p2pClient.Connect(agentParts[0], agentPort); err != nil {
		log.Error().Err(err).Msg("[dahua] failed to connect to agent for PTCP")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to connect to agent for PTCP: %w", err)
	}

	// Read agent response (Server Nat Info)
	log.Debug().Msg("[dahua] step 10: reading agent NAT info")
	_, err = p2pClient.ReadRaw(opts.Timeout)
	if err != nil {
		log.Error().Err(err).Msg("[dahua] failed to read agent PTCP info")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to read agent PTCP info: %w", err)
	}

	// Start PTCP session with agent
	agentSession := ptcp.NewSession()

	// Send PTCP SYNC to agent
	log.Debug().Msg("[dahua] step 10: PTCP sync with agent")
	syncPacket := agentSession.Send(ptcp.NewSyncBody())
	if err := p2pClient.Send(syncPacket.Serialize()); err != nil {
		log.Error().Err(err).Msg("[dahua] failed to send PTCP sync to agent")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to send PTCP sync to agent: %w", err)
	}

	// Read SYNC response
	syncResp, err := p2pClient.ReadRaw(opts.Timeout)
	if err != nil {
		log.Error().Err(err).Msg("[dahua] failed to read PTCP sync response from agent")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to read PTCP sync response from agent: %w", err)
	}

	syncRespPacket, err := ptcp.ParsePacket(syncResp)
	if err != nil {
		log.Error().Err(err).Msg("[dahua] failed to parse PTCP sync response")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to parse PTCP sync response: %w", err)
	}
	agentSession.Recv(syncRespPacket)

	log.Debug().Msg("[dahua] PTCP session established with agent")

	// Step 11: Request sign from agent (command 0x17)
	log.Debug().Msg("[dahua] step 11: requesting sign from agent")
	signRequestCmd := []byte{0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	signReqPacket := agentSession.Send(ptcp.NewCommandBody(signRequestCmd))
	if err := p2pClient.Send(signReqPacket.Serialize()); err != nil {
		log.Error().Err(err).Msg("[dahua] failed to send sign request")
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("failed to send sign request: %w", err)
	}

	var sign []byte
	for i := 0; i < 5; i++ {
		signResp, err := p2pClient.ReadRaw(opts.Timeout)
		if err != nil {
			log.Debug().Err(err).Int("attempt", i+1).Msg("[dahua] sign read timeout, resending request")
			signReqPacket = agentSession.Send(ptcp.NewCommandBody(signRequestCmd))
			if sendErr := p2pClient.Send(signReqPacket.Serialize()); sendErr != nil {
				log.Warn().Err(sendErr).Msg("[dahua] failed to resend sign request")
			}
			continue
		}
		signRespPacket, err := ptcp.ParsePacket(signResp)
		if err != nil {
			continue
		}
		agentSession.Recv(signRespPacket)

		if signRespPacket.Body.Type == ptcp.BodyTypeCommand && len(signRespPacket.Body.Command) > 12 {
			sign = signRespPacket.Body.Command[12:]
			break
		}
	}

	if sign == nil {
		mainClient.Close()
		p2pClient.Close()
		deviceClient.Close()
		return nil, fmt.Errorf("could not get sign from agent")
	}

	log.Debug().Hex("sign", sign).Msg("[dahua] got sign")

	// Step 12: Try direct connection first, then fall back to relay
	mainClient.Close()

	const directRetries = 3
	var session *ptcp.Session
	for attempt := 1; attempt <= directRetries; attempt++ {
		log.Debug().Int("attempt", attempt).Msg("[dahua] step 12: attempting direct connection")
		session, err = performDirectHandshake(deviceClient, cid, devicePubAddr, deviceLAddr, sign, opts)
		if err == nil {
			break
		}
		log.Warn().Err(err).Int("attempt", attempt).Int("max", directRetries).Msg("[dahua] direct connection attempt failed")
	}

	if err == nil {
		p2pClient.Close()
		log.Info().Str("serial", opts.Serial).Msg("[dahua] direct P2P connection established")
		return &HandshakeResult{
			Client:     deviceClient,
			Session:    session,
			DeviceIP:   deviceParts[0],
			DevicePort: devicePort,
		}, nil
	}

	// Direct connection failed -- fall back to relay mode via the agent
	log.Warn().Str("serial", opts.Serial).Msg("[dahua] direct connection failed, falling back to relay via agent")
	deviceClient.Close()

	// The agentSession is already established (steps 10-11). The p2pClient
	// is still connected to the agent. We send the auth sign (0x19) through
	// the relay to prove legitimacy, then use the agent as a relay tunnel.
	authCmd := []byte{0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	authCmd = append(authCmd, sign...)
	authPacket := agentSession.Send(ptcp.NewCommandBody(authCmd))
	if err := p2pClient.Send(authPacket.Serialize()); err != nil {
		p2pClient.Close()
		return nil, fmt.Errorf("relay auth send failed: %w", err)
	}

	for i := 0; i < 5; i++ {
		authResp, readErr := p2pClient.ReadRaw(opts.Timeout)
		if readErr != nil {
			continue
		}
		authRespPacket, parseErr := ptcp.ParsePacket(authResp)
		if parseErr != nil {
			continue
		}
		agentSession.Recv(authRespPacket)
		if authRespPacket.Body.Type == ptcp.BodyTypeCommand &&
			len(authRespPacket.Body.Command) > 0 &&
			authRespPacket.Body.Command[0] == 0x1A {
			break
		}
	}

	ackCmd := []byte{0x1b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	ackPacket := agentSession.Send(ptcp.NewCommandBody(ackCmd))
	if err := p2pClient.Send(ackPacket.Serialize()); err != nil {
		p2pClient.Close()
		return nil, fmt.Errorf("relay ack send failed: %w", err)
	}

	if ackResp, readErr := p2pClient.ReadRaw(opts.Timeout); readErr == nil {
		if ackRespPacket, parseErr := ptcp.ParsePacket(ackResp); parseErr == nil {
			agentSession.Recv(ackRespPacket)
		}
	}

	for i := 0; i < 3; i++ {
		extra, readErr := p2pClient.ReadRaw(500 * time.Millisecond)
		if readErr != nil {
			break
		}
		if pkt, parseErr := ptcp.ParsePacket(extra); parseErr == nil {
			agentSession.Recv(pkt)
		}
	}

	log.Info().Str("serial", opts.Serial).Msg("[dahua] relay P2P connection established via agent")

	return &HandshakeResult{
		Client:     p2pClient,
		Session:    agentSession,
		DeviceIP:   agentParts[0],
		DevicePort: agentPort,
	}, nil
}

// performDirectHandshake performs the STUN-like handshake and PTCP authentication with the device
func performDirectHandshake(client *UDPClient, cid []byte, devicePubAddr, deviceLAddr string, sign []byte, opts HandshakeOptions) (*ptcp.Session, error) {
	// Invert CID
	invertedCID := make([]byte, len(cid))
	for i, b := range cid {
		invertedCID[i] = ^b
	}

	// Generate cookie and transaction ID
	cookie := RandomBytes(4)
	transID := RandomBytes(12)

	// Build STUN request targeting the public address
	data := []byte{0xff, 0xfe, 0xff, 0xe7}
	data = append(data, cookie...)
	data = append(data, transID...)
	data = append(data, 0x7f, 0xd5, 0xff, 0xf7)
	data = append(data, invertedCID...)
	data = append(data, 0xff, 0xfb, 0xff, 0xf7, 0xff, 0xfe)
	data = append(data, ipToBytes(devicePubAddr)...)

	log.Debug().Str("pub", devicePubAddr).Str("local", deviceLAddr).Msg("[dahua] sending STUN request to device")

	if err := client.Send(data); err != nil {
		return nil, fmt.Errorf("failed to send STUN request: %w", err)
	}

	// Also send to the device's local/LAN address for same-network cameras
	if deviceLAddr != devicePubAddr && deviceLAddr != "" {
		localAddr, err := net.ResolveUDPAddr("udp", deviceLAddr)
		if err == nil {
			_ = client.SendTo(data, localAddr)
		}
	}

	// Read response from whichever address replies first (8s for WAN latency)
	resp, respAddr, err := client.RecvFrom(8 * time.Second)
	if err != nil {
		return nil, fmt.Errorf("timeout waiting for device response (try relay mode): %w", ErrTimeout)
	}

	// Reconnect to the address that actually responded
	if respAddr != nil {
		client.Connect(respAddr.IP.String(), respAddr.Port)
		log.Debug().Str("addr", respAddr.String()).Msg("[dahua] STUN response from")
	}

	if len(resp) < 20 {
		return nil, ErrInvalidResponse
	}

	// Extract remote transaction ID
	rtransID := resp[8:20]

	// Send STUN confirmation (magic 0xfefefff3, no trailing address)
	data2 := []byte{0xfe, 0xfe, 0xff, 0xf3}
	data2 = append(data2, cookie...)
	data2 = append(data2, rtransID...)
	data2 = append(data2, 0x7f, 0xd6, 0xff, 0xf7)
	data2 = append(data2, invertedCID...)

	if err := client.Send(data2); err != nil {
		return nil, fmt.Errorf("failed to send STUN confirmation: %w", err)
	}

	// Drain remaining STUN handshake packets (arrive within ~250ms)
	for i := 0; i < 5; i++ {
		_, err := client.ReadRaw(500 * time.Millisecond)
		if err != nil {
			break
		}
	}

	// Now perform PTCP handshake
	session := ptcp.NewSession()

	// Send SYNC
	syncPacket := session.Send(ptcp.NewSyncBody())
	if err := client.Send(syncPacket.Serialize()); err != nil {
		return nil, fmt.Errorf("failed to send PTCP sync: %w", err)
	}

	// Read SYNC response
	syncResp, err := client.ReadRaw(opts.Timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to read PTCP sync response: %w", err)
	}

	packet, err := ptcp.ParsePacket(syncResp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PTCP sync response: %w", err)
	}
	session.Recv(packet)

	if packet.Body.Type != ptcp.BodyTypeSync {
		return nil, errors.New("expected SYNC response")
	}

	// Send authentication with sign (command 0x19)
	authCmd := []byte{0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	authCmd = append(authCmd, sign...)
	authPacket := session.Send(ptcp.NewCommandBody(authCmd))
	if err := client.Send(authPacket.Serialize()); err != nil {
		return nil, fmt.Errorf("failed to send auth: %w", err)
	}

	// Read auth response (may need to skip empty packets)
	for i := 0; i < 5; i++ {
		authResp, err := client.ReadRaw(opts.Timeout)
		if err != nil {
			continue
		}
		authRespPacket, err := ptcp.ParsePacket(authResp)
		if err != nil {
			continue
		}
		session.Recv(authRespPacket)

		if authRespPacket.Body.Type == ptcp.BodyTypeEmpty {
			continue
		}

		if authRespPacket.Body.Type == ptcp.BodyTypeCommand {
			if len(authRespPacket.Body.Command) > 0 && authRespPacket.Body.Command[0] == 0x1A {
				// Auth successful
				break
			}
		}
	}

	// Send final acknowledgment (command 0x1B)
	ackCmd := []byte{0x1b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	ackPacket := session.Send(ptcp.NewCommandBody(ackCmd))
	if err := client.Send(ackPacket.Serialize()); err != nil {
		return nil, fmt.Errorf("failed to send ack: %w", err)
	}

	// Read final ack response and update session state so recv/rmid
	// counters stay in sync with the device (critical for subsequent packets)
	if ackResp, err := client.ReadRaw(opts.Timeout); err == nil {
		if ackRespPacket, err := ptcp.ParsePacket(ackResp); err == nil {
			session.Recv(ackRespPacket)
		}
	}

	// Drain any additional pending packets the device sent during the handshake
	for i := 0; i < 3; i++ {
		extra, err := client.ReadRaw(500 * time.Millisecond)
		if err != nil {
			break
		}
		if pkt, err := ptcp.ParsePacket(extra); err == nil {
			session.Recv(pkt)
		}
	}

	log.Debug().Msg("[dahua] PTCP session established with device")

	return session, nil
}
