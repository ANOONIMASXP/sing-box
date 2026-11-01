package group

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"

	"github.com/stretchr/testify/require"
)

func startRecoveryTestServer(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func shortenRecoveryDelays(t *testing.T) {
	t.Helper()
	previousDelay, previousBackoff := groupFailureProbeDelay, groupStartupProbeBackoff
	previousURLTestDelay, previousURLTestBackoff := urlTestFailureProbeDelay, urlTestStartupBackoff
	groupFailureProbeDelay = 10 * time.Millisecond
	groupStartupProbeBackoff = []time.Duration{10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond}
	urlTestFailureProbeDelay = 10 * time.Millisecond
	urlTestStartupBackoff = groupStartupProbeBackoff
	t.Cleanup(func() {
		groupFailureProbeDelay, groupStartupProbeBackoff = previousDelay, previousBackoff
		urlTestFailureProbeDelay, urlTestStartupBackoff = previousURLTestDelay, previousURLTestBackoff
	})
}

func TestRunStartupProbeBackoff(t *testing.T) {
	shortenRecoveryDelays(t)
	var probes atomic.Int32
	runStartupProbeBackoff(context.Background(), nil, time.Hour, func() bool { return false }, func() bool { return false }, func() bool {
		probes.Add(1)
		return false
	})
	require.EqualValues(t, len(groupStartupProbeBackoff), probes.Load())

	probes.Store(0)
	runStartupProbeBackoff(context.Background(), nil, time.Hour, func() bool { return false }, func() bool { return false }, func() bool {
		return probes.Add(1) == 2
	})
	require.EqualValues(t, 2, probes.Load(), "stops once a probe succeeds")

	probes.Store(0)
	runStartupProbeBackoff(context.Background(), nil, time.Hour, func() bool { return true }, func() bool { return false }, func() bool {
		probes.Add(1)
		return false
	})
	require.Zero(t, probes.Load(), "an idle group is not probed")

	probes.Store(0)
	runStartupProbeBackoff(context.Background(), nil, 5*time.Millisecond, func() bool { return false }, func() bool { return false }, func() bool {
		probes.Add(1)
		return false
	})
	require.EqualValues(t, 1, probes.Load(), "retries never exceed the interval")
}

func TestLoadBalanceFailedMemberIsDroppedAndInterrupted(t *testing.T) {
	shortenRecoveryDelays(t)
	history := urltest.NewHistoryStorage()
	dead := &probeTestOutbound{tag: "dead"}
	alive := &probeTestOutbound{tag: "alive"}
	dead.failing.Store(true)
	group, err := NewLoadBalanceGroup(context.Background(), nil, log.NewNOPFactory().Logger(), []adapter.Outbound{dead, alive}, startRecoveryTestServer(t), time.Minute, time.Hour, 0, StrategyRoundRobin)
	require.NoError(t, err)
	group.history = history
	group.lastActive.Store(time.Now())
	for _, member := range []adapter.Outbound{dead, alive} {
		history.StoreURLTestHistory(member.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 100})
	}
	deadConn, peer := net.Pipe()
	defer peer.Close()
	tracked := group.newConn(dead.Tag(), deadConn, false, false)
	aliveConn, alivePeer := net.Pipe()
	defer alivePeer.Close()
	aliveTracked := group.newConn(alive.Tag(), aliveConn, false, false)
	defer aliveTracked.Close()

	group.onOutboundFailed(dead)

	require.Eventually(t, func() bool {
		return history.LoadURLTestHistory(dead.Tag()) == nil
	}, 5*time.Second, 10*time.Millisecond)
	_, err = tracked.Read(make([]byte, 1))
	require.Error(t, err, "connections through the dead member are closed")
	require.NotNil(t, history.LoadURLTestHistory(alive.Tag()))
	aliveConn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	_, err = aliveTracked.Read(make([]byte, 1))
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout(), "connections through other members stay open")
}
