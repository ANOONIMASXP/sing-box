package group

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

// Smart historically used a legacy Start() error signature that this
// branch's outbound manager only discovers through a runtime
// adapter.Lifecycle assertion — a mismatch fails silently and the group
// never starts (no member snapshot, no PostStart, no background tasks,
// s.started stays false). Pin the staged dispatch so a regression breaks
// the test instead of production.
func TestSmartStartStagedLifecycle(t *testing.T) {
	logger := log.NewNOPFactory().NewLogger("smart")
	leaf := &stubOutbound{tag: "leaf"}
	manager := &loadBalanceURLTestOutboundManager{outbounds: map[string]adapter.Outbound{"leaf": leaf}}
	ctx := service.ContextWith[adapter.OutboundManager](context.Background(), manager)

	scope := adapter.NewScope(ctx, logger)
	t.Cleanup(func() { _ = scope.Close() })

	raw, err := NewSmart(ctx, nil, logger, "smart", option.SmartOutboundOptions{
		GroupCommonOption: option.GroupCommonOption{Outbounds: []string{"leaf"}},
	})
	require.NoError(t, err)
	s := raw.(*Smart)

	// The outbound manager resolves lifecycle components with this exact
	// interface; a legacy Start() signature would fail the assertion and
	// silently skip the group.
	var lifecycle adapter.Lifecycle = s
	require.Nil(t, s.state.Load(), "member snapshot must not exist before Start")

	require.NoError(t, lifecycle.Start(adapter.StartStateStart, scope))
	require.NotNil(t, s.state.Load(), "start stage must build the member snapshot")
	require.False(t, s.started.Load(), "started flag must stay false before PostStart")

	require.NoError(t, lifecycle.Start(adapter.StartStateStarted, scope))
	require.True(t, s.started.Load(), "finish-start stage must run PostStart and mark the group started")
}
