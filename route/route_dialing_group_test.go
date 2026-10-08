package route

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

// resolveOutbound must stop at dial-through groups (URLTest / Smart /
// smart-region): the group dials its own member so selection, learning,
// breakers and failover observe every dial, and the dial trace appends the
// member the group actually picked. Unwrapping to the leaf here would hand
// the connection straight to the node, bypassing the group entirely, and
// would also keep NeedOutboundDialTrace from ever arming the chain trace.
func TestResolveOutboundStopsAtDialingGroup(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNOPFactory().NewLogger("test")
	scope := adapter.NewScope(ctx, logger)
	t.Cleanup(func() { require.NoError(t, scope.Close()) })

	leaf := &selectorInterruptTestOutbound{
		Adapter: outbound.NewAdapter("test", "leaf", []string{N.NetworkTCP, N.NetworkUDP}, nil),
	}
	outbounds := map[string]adapter.Outbound{"leaf": leaf}
	manager := &testL3OutboundManager{outbounds: outbounds}
	groupCtx := service.ContextWith[adapter.OutboundManager](ctx, manager)
	groupCtx = service.ContextWithPtr(groupCtx, urltest.NewHistoryStorage())

	rawURLTest, err := group.NewURLTest(groupCtx, nil, logger, "urltest", option.URLTestOutboundOptions{
		GroupCommonOption: option.GroupCommonOption{Outbounds: []string{"leaf"}},
	})
	require.NoError(t, err)
	urlTestGroup := rawURLTest.(*group.URLTest)
	require.NoError(t, urlTestGroup.Start(adapter.StartStateStart, scope))
	outbounds["urltest"] = urlTestGroup

	metadata := adapter.InboundContext{Destination: M.Socksaddr{Fqdn: "example.com", Port: 443}}

	// The dialing group itself is the final chain element.
	chain, err := resolveOutbound(urlTestGroup, N.NetworkTCP, &metadata)
	require.NoError(t, err)
	require.Len(t, chain, 1)
	require.Equal(t, "urltest", chain[0].Tag())

	// Nested: a selector unwraps down to the dialing group, then stops.
	rawSelector, err := group.NewSelector(groupCtx, nil, logger, "selector", option.SelectorOutboundOptions{
		GroupCommonOption: option.GroupCommonOption{Outbounds: []string{"urltest"}},
	})
	require.NoError(t, err)
	selectorGroup := rawSelector.(*group.Selector)
	require.NoError(t, selectorGroup.Start(adapter.StartStateStart, scope))

	chain, err = resolveOutbound(selectorGroup, N.NetworkTCP, &metadata)
	require.NoError(t, err)
	require.Len(t, chain, 2)
	require.Equal(t, "selector", chain[0].Tag())
	require.Equal(t, "urltest", chain[1].Tag())
}
