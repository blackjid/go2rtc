package tunnel

import (
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/dahua/ptcp"
)

func payloadAt(sent uint32) *ptcp.Packet {
	// 12-byte payload framing + 88 bytes: each body spans 100 offsets.
	return ptcp.NewPacket(&ptcp.Header{Sent: sent}, ptcp.NewPayloadBody(1, make([]byte, 88)))
}

// Under overlapping holes the clock must time the hole the stream is stuck at
// now, not the first one: skipping on the first hole's clock drops bytes the
// device was only just asked to resend.
func TestGapClockFollowsCurrentHole(t *testing.T) {
	s := ptcp.NewSession()
	tun := &Tunnel{session: s}

	s.Receive(payloadAt(0))   // recv = 100
	s.Receive(payloadAt(200)) // hole A at 100
	tun.trackGap()
	if tun.gapSince.IsZero() || tun.gapAt != 100 {
		t.Fatalf("clock not started on hole A: since=%v at=%d", tun.gapSince, tun.gapAt)
	}
	s.Receive(payloadAt(400)) // hole B at 300, behind A

	tun.gapSince = time.Now().Add(-gapSkipAfter) // hole A is old
	s.Receive(payloadAt(100))                    // A fills; stuck at B now
	tun.trackGap()
	if tun.gapAt != 300 || time.Since(tun.gapSince) > time.Second {
		t.Fatalf("clock still on hole A: at=%d age=%s", tun.gapAt, time.Since(tun.gapSince))
	}

	s.Receive(payloadAt(300)) // B fills; nothing pending
	tun.trackGap()
	if !tun.gapSince.IsZero() {
		t.Fatalf("clock still running with no hole")
	}
}
