//go:build with_ebpf && (linux || android)

package dialer

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"

	commonEBPF "github.com/ANOONIMASXP/sing-ebpf"
	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	N "github.com/sagernet/sing/common/network"

	"golang.org/x/sys/unix"
)

type selfBypassTestNetwork struct {
	adapter.NetworkManager
	tracker *commonEBPF.SelfBypass
}

func (n *selfBypassTestNetwork) SetEBPFSelfBypass(tracker *commonEBPF.SelfBypass) error {
	n.tracker = tracker
	return nil
}

func (n *selfBypassTestNetwork) ClearEBPFSelfBypass(tracker *commonEBPF.SelfBypass) {
	if n.tracker == tracker {
		n.tracker = nil
	}
}

func TestPrepareEBPFSelfBypassRequiresScope(t *testing.T) {
	network := new(selfBypassTestNetwork)
	inbounds := []option.Inbound{{Type: C.TypeEBPF, Options: &option.EBPFInboundOptions{}}}
	if err := PrepareEBPFSelfBypass(network, inbounds, nil); err == nil {
		t.Fatal("accepted a kernel resource without a lifecycle scope")
	}
	if network.tracker != nil {
		t.Fatal("allocated self-bypass before validating scope")
	}
}

func TestPrepareEBPFSelfBypassClosesWithoutInboundStart(t *testing.T) {
	network := new(selfBypassTestNetwork)
	scope := adapter.NewScope(context.Background(), log.NewNOPFactory().Logger())
	t.Cleanup(func() { _ = scope.Close() })
	inbounds := []option.Inbound{{Type: C.TypeEBPF, Options: &option.EBPFInboundOptions{}}}
	if err := PrepareEBPFSelfBypass(network, inbounds, scope); err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.ENOSYS) {
			t.Skipf("kernel BPF map creation unavailable: %v", err)
		}
		t.Fatal(err)
	}
	tracker := network.tracker
	if tracker == nil || tracker.IsClosed() {
		t.Fatal("self-bypass map was not prepared")
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if !tracker.IsClosed() || network.tracker != nil {
		t.Fatal("unstarted self-bypass map survived scope cleanup")
	}
}

type halfCloseTestConn struct {
	net.Conn
	readClosed, writeClosed bool
	closes                  int
}

func (c *halfCloseTestConn) CloseRead() error  { c.readClosed = true; return nil }
func (c *halfCloseTestConn) CloseWrite() error { c.writeClosed = true; return nil }
func (c *halfCloseTestConn) Close() error      { c.closes++; return nil }

func TestSelfBypassPreservesHalfClose(t *testing.T) {
	underlying := &halfCloseTestConn{}
	cleanups := 0
	wrapped := &selfBypassConn{Conn: underlying, cleanup: func() { cleanups++ }}
	if err := N.CloseWrite(wrapped); err != nil {
		t.Fatal(err)
	}
	if err := N.CloseRead(wrapped); err != nil {
		t.Fatal(err)
	}
	if !underlying.writeClosed || !underlying.readClosed || cleanups != 0 || underlying.closes != 0 {
		t.Fatal("half close must reach the socket without unregistering it")
	}
	if N.UnwrapReader(wrapped) != underlying || N.UnwrapWriter(wrapped) != underlying {
		t.Fatal("I/O capabilities must remain accessible")
	}
	var wait sync.WaitGroup
	for range 8 {
		wait.Go(func() { _ = wrapped.Close() })
	}
	wait.Wait()
	if cleanups != 1 || underlying.closes != 1 {
		t.Fatal("full close must run exactly once")
	}
}

type noSelfBypassNetworkManager struct{ adapter.NetworkManager }

func (noSelfBypassNetworkManager) EBPFSelfBypass() *commonEBPF.SelfBypass { return nil }

func TestSelfBypassWithoutLocalInboundIsInert(t *testing.T) {
	if AppendEBPFSelfBypass(noSelfBypassNetworkManager{}, nil) != nil {
		t.Fatal("no tracker must mean no socket control callback")
	}
}

type socketAssignNetworkManager struct {
	adapter.NetworkManager
	enabled bool
}

func (m socketAssignNetworkManager) EBPFLocalSocketAssign() bool { return m.enabled }

func TestOutboundPortReservationOnlyForLocalSocketAssign(t *testing.T) {
	if appendEBPFOutboundPortReservation(socketAssignNetworkManager{}, nil) != nil {
		t.Fatal("cgroup or shared-only eBPF must keep connect-time port selection")
	}
	if appendEBPFOutboundPortReservation(noSelfBypassNetworkManager{}, nil) != nil {
		t.Fatal("a network manager without the capability must keep connect-time port selection")
	}
	controlFunc := appendEBPFOutboundPortReservation(socketAssignNetworkManager{enabled: true}, nil)
	if controlFunc == nil {
		t.Fatal("local socket assignment must reserve outbound ports")
	}
	for _, network := range []string{"tcp4", "tcp6"} {
		address := "127.0.0.1:0"
		if network == "tcp6" {
			address = "[::1]:0"
		}
		listener, err := net.Listen(network, address)
		if err != nil {
			t.Skipf("listen %s: %v", network, err)
		}
		defer listener.Close()
		var reuse int
		dialer := net.Dialer{Control: func(network, address string, conn syscall.RawConn) error {
			if err := controlFunc(network, address, conn); err != nil {
				return err
			}
			return conn.Control(func(fd uintptr) {
				reuse, _ = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR)
			})
		}}
		conn, err := dialer.Dial(network, listener.Addr().String())
		if err != nil {
			t.Fatalf("dial %s with a reserved port: %v", network, err)
		}
		if reuse != 1 || conn.LocalAddr().(*net.TCPAddr).Port == 0 {
			t.Fatalf("%s: SO_REUSEADDR=%d local=%v", network, reuse, conn.LocalAddr())
		}
		conn.Close()
	}
	// UDP keeps its own binding rules.
	if err := reserveEBPFOutboundPort("udp4", "", nil); err != nil {
		t.Fatal(err)
	}
}
