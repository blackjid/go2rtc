package rtsp

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/tcp"
)

func TestClientDialerCalledForEveryDial(t *testing.T) {
	var calls int
	var peers []net.Conn
	client := NewClientWithDialer("rtsp://user:pass@tunnel/stream", func() (net.Conn, error) {
		calls++
		conn, peer := net.Pipe()
		peers = append(peers, peer)
		return conn, nil
	})
	t.Cleanup(func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	})

	for i := 1; i <= 2; i++ {
		if err := client.Dial(); err != nil {
			t.Fatalf("Dial %d: %v", i, err)
		}
		if calls != i {
			t.Fatalf("dialer calls after Dial %d = %d, want %d", i, calls, i)
		}
		_ = client.conn.Close()
	}
}

func TestClientCommandTimeout(t *testing.T) {
	conn := newDeadlineConn()
	client := NewClientWithDialer("rtsp://tunnel/stream", func() (net.Conn, error) {
		return conn, nil
	})
	client.CmdTimeout = 30 * time.Second

	if err := client.Dial(); err != nil {
		t.Fatal(err)
	}
	if err := client.WriteRequest(&tcp.Request{Method: MethodOptions, URL: client.URL}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReadResponse(); err != io.EOF {
		t.Fatalf("ReadResponse error = %v, want EOF", err)
	}

	if got := conn.writeDeadline.Sub(conn.writeDeadlineSet); got < client.CmdTimeout-time.Second || got > client.CmdTimeout {
		t.Fatalf("write timeout = %s, want approximately %s", got, client.CmdTimeout)
	}
	if got := conn.readDeadline.Sub(conn.readDeadlineSet); got < client.CmdTimeout-time.Second || got > client.CmdTimeout {
		t.Fatalf("read timeout = %s, want approximately %s", got, client.CmdTimeout)
	}
}

type deadlineConn struct {
	readDeadline     time.Time
	readDeadlineSet  time.Time
	writeDeadline    time.Time
	writeDeadlineSet time.Time
}

func newDeadlineConn() *deadlineConn { return &deadlineConn{} }

func (c *deadlineConn) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *deadlineConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *deadlineConn) Close() error                { return nil }
func (c *deadlineConn) LocalAddr() net.Addr         { return pipeAddr("local") }
func (c *deadlineConn) RemoteAddr() net.Addr        { return pipeAddr("remote") }
func (c *deadlineConn) SetDeadline(t time.Time) error {
	c.readDeadlineSet = time.Now()
	c.readDeadline = t
	c.writeDeadlineSet = c.readDeadlineSet
	c.writeDeadline = t
	return nil
}
func (c *deadlineConn) SetReadDeadline(t time.Time) error {
	c.readDeadlineSet = time.Now()
	c.readDeadline = t
	return nil
}
func (c *deadlineConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadlineSet = time.Now()
	c.writeDeadline = t
	return nil
}

type pipeAddr string

func (a pipeAddr) Network() string { return "test" }
func (a pipeAddr) String() string  { return string(a) }
