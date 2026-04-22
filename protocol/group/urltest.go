package group

import (
	"context"
	"io"
	"maps"
	"net"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/batch"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

// manualPinData holds the user's temporary manual selection on a URLTest group.
// The pin is released automatically when the user triggers another manual speed
// test (clash API /group/{name}/delay or libbox URLTest RPC), so periodic
// health checks and dial-failure auto-rechecks never silently unpin.
type manualPinData struct {
	tag      string
	outbound adapter.Outbound
}

const (
	maxScreeningConcurrency = 16 // Phase 1: coarse screening — balanced speed vs memory
	maxPrecisionConcurrency = 2  // Phase 2: precision retest — minimal concurrency for accuracy
	maxPrecisionCandidates  = 8  // Number of top candidates to retest
	maxFailoverCandidates   = 10

	// Consecutive health-check failures before we stop trusting a node.
	// Unlike mihomo, we don't delete history — we mark it stale by zeroing delay.
	// This keeps the node in the ranked list for failover but deprioritizes it.
	healthCheckFailThreshold = 3

	// Dial failures that trigger a proactive re-check (mihomo parity).
	dialFailureThreshold = 5

	// When current selection's history is missing, how long to trust it anyway
	// before considering a switch. Prevents flapping on transient test-URL blockage.
	staleHistoryGrace = 2 * time.Minute
)

func RegisterURLTest(registry *outbound.Registry) {
	outbound.Register[option.URLTestOutboundOptions](registry, C.TypeURLTest, NewURLTest)
}

var (
	_ adapter.PreMatchOutboundGroup   = (*URLTest)(nil)
	_ adapter.InterfaceUpdateListener = (*URLTest)(nil)
	_ adapter.Referrer                = (*URLTest)(nil)
)

type URLTest struct {
	outbound.Adapter
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	logger                       log.ContextLogger
	tags                         []string
	link                         string
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	fallback                     URLTestFallback
	group                        *URLTestGroup
	checkAccess                  sync.Mutex
	interruptExternalConnections bool
	providerAccess               sync.Mutex
	providerUpdateCheck          providerUpdateCheckScheduler
	// expectedStatus mihomo 对齐的状态码 matcher。nil = 旧启发式。
	// NewURLTest 时 Parse，失败阻断启动；loopCheckOutbounds 里每次探测透传。
	expectedStatus *urltest.StatusMatcher

	provider       adapter.ProviderManager
	providers      map[string]adapter.Provider
	outboundsCache map[string][]adapter.Outbound

	providerTags    []string
	exclude         *regexp.Regexp
	include         *regexp.Regexp
	useAllProviders bool
	hidden          bool
	icon            string
}

type URLTestFallback struct {
	enabled  bool
	maxDelay int64
}

func NewURLTest(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.URLTestOutboundOptions) (adapter.Outbound, error) {
	outbound := &URLTest{
		Adapter:                      outbound.NewAdapter(C.TypeURLTest, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:                          ctx,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		logger:                       logger,
		tags:                         options.Outbounds,
		link:                         options.URL,
		interval:                     time.Duration(options.Interval),
		tolerance:                    options.Tolerance,
		idleTimeout:                  time.Duration(options.IdleTimeout),
		interruptExternalConnections: options.InterruptExistConnections,

		provider:       service.FromContext[adapter.ProviderManager](ctx),
		providers:      make(map[string]adapter.Provider),
		outboundsCache: make(map[string][]adapter.Outbound),

		providerTags:    options.Providers,
		exclude:         (*regexp.Regexp)(options.Exclude),
		include:         (*regexp.Regexp)(options.Include),
		useAllProviders: options.UseAllProviders,
		hidden:          options.Hidden,
		icon:            options.Icon,
	}
	if options.Fallback.Enabled {
		outbound.fallback = URLTestFallback{
			enabled:  true,
			maxDelay: time.Duration(options.Fallback.MaxDelay).Milliseconds(),
		}
	}
	// 解析 expected_status。用户写错立即报错，避免启动后每次探测才发现。
	matcher, err := urltest.ParseExpectedStatus(options.ExpectedStatus)
	if err != nil {
		return nil, err
	}
	outbound.expectedStatus = matcher
	return outbound, nil
}

// Hidden / Icon expose the dashboard hints from option.GroupCommonOption.
// See adapter.OutboundGroup interface for the semantic contract.
func (s *URLTest) Hidden() bool { return s.hidden }
func (s *URLTest) Icon() string { return s.icon }

func (s *URLTest) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage != adapter.StartStateStart {
		if stage == adapter.StartStateStarted {
			s.group.PostStart()
		}
		return nil
	}
	s.providerAccess.Lock()
	defer s.providerAccess.Unlock()
	if s.useAllProviders {
		var providerTags []string
		for _, provider := range s.provider.Providers() {
			providerTags = append(providerTags, provider.Tag())
			s.providers[provider.Tag()] = provider
		}
		s.providerTags = providerTags
	} else {
		for i, tag := range s.providerTags {
			provider, loaded := s.provider.Get(tag)
			if !loaded {
				return E.New("outbound provider ", i, " not found: ", tag)
			}
			s.providers[tag] = provider
		}
	}
	if len(s.tags)+len(s.providerTags) == 0 {
		return E.New("missing outbound and provider tags")
	}

	outbounds := make([]adapter.Outbound, 0, len(s.tags))
	for i, tag := range s.tags {
		detour, loaded := s.outbound.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		outbounds = append(outbounds, detour)
	}
	if len(s.tags) == 0 {
		detour, _ := s.outbound.Outbound("Compatible")
		s.tags = append(s.tags, detour.Tag())
		outbounds = append(outbounds, detour)
	}
	group, err := NewURLTestGroup(s.ctx, s.outbound, s.logger, outbounds, s.link, s.interval, s.tolerance, s.idleTimeout, s.fallback, s.interruptExternalConnections, s.expectedStatus)
	if err != nil {
		return err
	}
	s.group = group
	scope.Add(group.Close)
	for _, providerTag := range s.providerTags {
		p := s.providers[providerTag]
		element := p.RegisterCallback(s.onProviderUpdated)
		scope.Add(func() error { p.UnregisterCallback(element); return nil })
	}
	return nil
}

// Now returns the pin tag if set, otherwise the cached per-network selection.
// Mirrors mihomo's Now() so Clash-API consumers can read the effective member
// uniformly across group types.
func (s *URLTest) Now() string {
	if pin := s.group.manualPin.Load(); pin != nil {
		return pin.tag
	}
	if tcp := s.group.selectedOutboundTCP.Load(); tcp != nil {
		return tcp.Tag()
	} else if udp := s.group.selectedOutboundUDP.Load(); udp != nil {
		return udp.Tag()
	}
	return ""
}

// Selected reports the user's manually-pinned outbound tag, or "" when the
// group is running on automatic selection. Mirrors Smart.Selected semantics so
// dashboards can render the pinned / fixed state uniformly across group types.
func (s *URLTest) Selected() string {
	if pin := s.group.manualPin.Load(); pin != nil {
		return pin.tag
	}
	return ""
}

// SelectOutbound pins a node as the temporary manual selection, or clears the
// pin when tag == "". Returns false only when tag is non-empty and not a
// member of the current group snapshot. The pin survives periodic health
// checks and dial-failure-triggered rechecks, and is released automatically on
// the next user-triggered URL test (see URLTestGroup.URLTest /
// URLTest.CheckOutbounds). New dials see the pin immediately; active
// connections are interrupted when interruptExistConnections is enabled
// (mirrors Selector.SelectOutbound).
func (s *URLTest) SelectOutbound(tag string) bool {
	if tag == "" {
		if s.group.clearManualPin() {
			s.logger.Info("manual pin released")
			if s.interruptExternalConnections {
				s.group.interruptGroup.Interrupt(true)
			}
		}
		return true
	}
	detour := s.group.findOutboundByTag(tag)
	if detour == nil {
		return false
	}
	prev := s.group.manualPin.Swap(&manualPinData{tag: tag, outbound: detour})
	if prev != nil && prev.outbound == detour {
		return true
	}
	s.logger.Info("manual pin set to ", tag)
	// Make the pin visible to cached-select fast paths and any code reading
	// selectedOutboundTCP/UDP directly (DialContext/ListenPacket). Only
	// overwrite when the pin supports the network so UDP-only / TCP-only
	// nodes don't poison the other-network cached slot.
	if common.Contains(detour.Network(), N.NetworkTCP) {
		s.group.selectedOutboundTCP.Store(detour)
	}
	if common.Contains(detour.Network(), N.NetworkUDP) {
		s.group.selectedOutboundUDP.Store(detour)
	}
	s.group.interruptGroup.Interrupt(s.interruptExternalConnections)
	return true
}

func (s *URLTest) All() []string {
	outbounds := s.group.loadOutbounds()
	tags := make([]string, 0, len(outbounds))
	for _, detour := range outbounds {
		tags = append(tags, detour.Tag())
	}
	return tags
}

func (s *URLTest) Selected(network string) adapter.Outbound {
	var outbound adapter.Outbound
	if network == N.NetworkUDP {
		outbound = s.group.selectedOutboundUDP.Load()
	} else {
		outbound = s.group.selectedOutboundTCP.Load()
	}
	if outbound == nil {
		outbound, _ = s.group.Select(network)
	}
	return outbound
}

func (s *URLTest) AttachConnection(closer io.Closer) func() {
	s.group.Touch()
	return s.group.interruptGroup.Add(closer, true)
}

func (s *URLTest) References() []string {
	group := s.group
	if group == nil {
		return nil
	}
	var references []string
	selectedOutboundTCP := group.selectedOutboundTCP.Load()
	selectedOutboundUDP := group.selectedOutboundUDP.Load()
	if selectedOutboundTCP != nil {
		references = append(references, selectedOutboundTCP.Tag())
	}
	if selectedOutboundUDP != nil && selectedOutboundUDP != selectedOutboundTCP {
		references = append(references, selectedOutboundUDP.Tag())
	}
	return references
}

func (s *URLTest) SelectPreMatchOutbound(metadata *adapter.InboundContext, selectOutbound func(adapter.Outbound) (adapter.Outbound, adapter.PreMatchAction)) (adapter.Outbound, adapter.PreMatchAction) {
	s.group.Touch()
	network := metadata.Network
	if network == N.NetworkICMP {
		network = N.NetworkTCP
	}
	return selectOutbound(s.Selected(network))
}

func (s *URLTest) URLTest(ctx context.Context) (map[string]uint16, error) {
	// User-triggered manual test — release the manual pin so selection
	// returns to the best measured node after this cycle completes.
	if s.group.clearManualPin() {
		s.logger.Info("manual pin released by user speed test")
	}
	return s.group.URLTest(ctx)
}

func (s *URLTest) urlTest(ctx context.Context, force bool) (map[string]uint16, error) {
	return s.group.urlTest(ctx, force)
}

func (s *URLTest) CheckOutbounds() {
	// User-triggered via gRPC/libbox — release the manual pin before
	// running. Internal callers use g.CheckOutbounds directly and must
	// preserve the pin.
	if s.group.clearManualPin() {
		s.logger.Info("manual pin released by user speed test")
	}
	s.group.CheckOutbounds(s.ctx, true)
}

func (s *URLTest) PerformUpdateCheck() {
	s.group.performUpdateCheck()
}

func (s *URLTest) InterfaceUpdated(ctx context.Context) {
	group := s.group
	if group == nil {
		return
	}
	if group.pause.IsDevicePaused() || group.pause.IsNetworkPaused() {
		return
	}
	go func() {
		s.checkAccess.Lock()
		defer s.checkAccess.Unlock()
		if ctx.Err() != nil {
			return
		}
		group.CheckOutbounds(ctx, true)
	}()

func (s *URLTest) isGroupActive() bool {
	if !s.group.started.Load() {
		return false
	}
	return time.Since(s.group.lastActive.Load()) <= s.group.idleTimeout
}

func (s *URLTest) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	s.group.Touch()
	var outbound adapter.Outbound
	switch N.NetworkName(network) {
	case N.NetworkTCP, N.NetworkUDP:
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
	if pinned := s.group.pinnedOutbound(N.NetworkName(network)); pinned != nil {
		outbound = pinned
	} else {
		switch N.NetworkName(network) {
		case N.NetworkTCP:
			outbound = s.group.selectedOutboundTCP.Load()
		case N.NetworkUDP:
			outbound = s.group.selectedOutboundUDP.Load()
		}
	}
	if outbound == nil {
		outbound, _ = s.group.Select(network)
	}
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := outbound.DialContext(ctx, network, destination)
	if err == nil {
		return s.group.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx), interrupt.IsResourceDownloadFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	s.group.history.DeleteURLTestHistory(outbound.Tag())
	return nil, err
}

func (s *URLTest) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	s.group.Touch()
	var outbound adapter.Outbound
	if pinned := s.group.pinnedOutbound(N.NetworkUDP); pinned != nil {
		outbound = pinned
	} else {
		outbound = s.group.selectedOutboundUDP.Load()
	}
	if outbound == nil {
		outbound, _ = s.group.Select(N.NetworkUDP)
	}
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := outbound.ListenPacket(ctx, destination)
	if err == nil {
		return s.group.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx), interrupt.IsResourceDownloadFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	s.group.history.DeleteURLTestHistory(outbound.Tag())
	return nil, err
}

func (s *URLTest) onProviderUpdated(tag string) error {
	s.providerAccess.Lock()
	_, outbounds, outboundsCache, err := collectProviderOutbounds(
		tag,
		s.Dependencies(),
		s.outbound,
		s.providers,
		s.providerTags,
		s.outboundsCache,
		s.exclude,
		s.include,
	)
	if err != nil {
		s.providerAccess.Unlock()
		return E.Cause(err, s.Tag())
	}
	s.outboundsCache = outboundsCache
	s.group.replaceOutbounds(outbounds)
	s.providerAccess.Unlock()
	if s.isGroupActive() {
		s.group.access.Lock()
		if s.group.ticker != nil {
			s.group.ticker.Reset(s.group.interval)
		}
		s.group.access.Unlock()
		s.providerUpdateCheck.Schedule(func() {
			_, _ = s.group.urlTestWait(s.ctx, false)
		})
	}
	return nil
}

type URLTestGroup struct {
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	pause                        pause.Manager
	pauseCallback                *list.Element[pause.Callback]
	logger                       log.Logger
	outbounds                    []adapter.Outbound
	outboundsAccess              sync.RWMutex
	link                         string
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	history                      *urltest.HistoryStorage
	checking                     sync.Mutex
	selectedOutboundTCP          common.TypedValue[adapter.Outbound]
	selectedOutboundUDP          common.TypedValue[adapter.Outbound]
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
	access                       sync.Mutex
	updateAccess                 sync.Mutex
	ticker                       *time.Ticker
	close                        chan struct{}
	started                      atomic.Bool
	lastActive                   common.TypedValue[time.Time]

	fallback URLTestFallback

	// expectedStatus: 上层 URLTest.NewURLTest 在构造 group 之前解析好
	// 再传进来；每次 URL 探测透传给 urltest.URLTestWithStatus。nil = 旧启发式。
	expectedStatus *urltest.StatusMatcher

	// manualPin: user's temporary manual selection. nil = auto.
	// Set via SelectOutbound, cleared at the next user-triggered URL test.
	manualPin atomic.Pointer[manualPinData]
}

func NewURLTestGroup(ctx context.Context, outboundManager adapter.OutboundManager, logger log.Logger, outbounds []adapter.Outbound, link string, interval time.Duration, tolerance uint16, idleTimeout time.Duration, fallback URLTestFallback, interruptExternalConnections bool, expectedStatus *urltest.StatusMatcher) (*URLTestGroup, error) {
	if interval == 0 {
		interval = C.DefaultURLTestInterval
	}
	if tolerance == 0 {
		tolerance = 50
	}
	if idleTimeout == 0 {
		idleTimeout = C.DefaultURLTestIdleTimeout
	}
	if interval > idleTimeout {
		return nil, E.New("interval must be less or equal than idle_timeout")
	}
	history := service.PtrFromContext[urltest.HistoryStorage](ctx)
	if history == nil {
		return nil, E.New("missing URL test history storage")
	}
	group := &URLTestGroup{
		ctx:                          ctx,
		outbound:                     outboundManager,
		logger:                       logger,
		link:                         link,
		interval:                     interval,
		tolerance:                    tolerance,
		idleTimeout:                  idleTimeout,
		history:                      history,
		fallback:                     fallback,
		close:                        make(chan struct{}),
		pause:                        service.FromContext[pause.Manager](ctx),
		interruptGroup:               interrupt.NewGroup(),
		interruptExternalConnections: interruptExternalConnections,
		expectedStatus:               expectedStatus,
	}
	group.storeOutbounds(outbounds)
	return group, nil
}

func (g *URLTestGroup) PostStart() {
	g.access.Lock()
	defer g.access.Unlock()
	g.started.Store(true)
	g.lastActive.Store(time.Now())
	go g.CheckOutbounds(g.ctx, false)
}

func (g *URLTestGroup) Touch() {
	if !g.started.Load() {
		return
	}
	g.access.Lock()
	defer g.access.Unlock()
	if g.ticker != nil {
		g.lastActive.Store(time.Now())
		return
	}
	ticker := time.NewTicker(g.interval)
	g.ticker = ticker
	g.pauseCallback = pause.RegisterTicker(g.pause, ticker, g.interval, nil)
	go g.loopCheck(ticker, g.close)
}

func (g *URLTestGroup) Close() error {
	g.access.Lock()
	defer g.access.Unlock()
	if g.ticker == nil {
		return nil
	}
	g.ticker.Stop()
	g.ticker = nil
	g.pause.UnregisterCallback(g.pauseCallback)
	g.pauseCallback = nil
	close(g.close)
	return nil
}

// pinnedOutbound returns the manually-pinned outbound when it is still a
// member of the current snapshot and supports the requested network; nil
// otherwise. When the pin has been removed from the snapshot (provider
// update dropped it), the pin is auto-cleared so selection falls back to
// automatic.
func (g *URLTestGroup) pinnedOutbound(network string) adapter.Outbound {
	pin := g.manualPin.Load()
	if pin == nil {
		return nil
	}
	if !containsOutbound(g.loadOutbounds(), pin.outbound) {
		g.manualPin.CompareAndSwap(pin, nil)
		return nil
	}
	if network != "" && !common.Contains(pin.outbound.Network(), network) {
		return nil
	}
	return pin.outbound
}

// clearManualPin drops the pin if one is set. Returns true when a pin was
// actually cleared so callers can log / interrupt conditionally.
func (g *URLTestGroup) clearManualPin() bool {
	return g.manualPin.Swap(nil) != nil
}

// findOutboundByTag walks the current snapshot looking for tag. Used by
// SelectOutbound so the pin only accepts tags that actually belong to this
// group (mirrors Selector.SelectOutbound's membership guard).
func (g *URLTestGroup) findOutboundByTag(tag string) adapter.Outbound {
	for _, o := range g.loadOutbounds() {
		if o.Tag() == tag {
			return o
		}
	}
	return nil
}

func (g *URLTestGroup) Select(network string) (adapter.Outbound, bool) {
	if pin := g.pinnedOutbound(network); pin != nil {
		return pin, true
	}
	var minDelay uint16
	var minOutbound adapter.Outbound
	var fallbackIgnoreOutboundDelay uint16
	var fallbackIgnoreOutbound adapter.Outbound
	if !g.fallback.enabled {
		switch network {
		case N.NetworkTCP:
			selectedOutbound := g.selectedOutboundTCP.Load()
			if selectedOutbound != nil {
				if history := g.history.LoadURLTestHistory(RealTag(selectedOutbound, network)); history != nil {
					minOutbound = selectedOutbound
					minDelay = history.Delay
				}
			}
		case N.NetworkUDP:
			selectedOutbound := g.selectedOutboundUDP.Load()
			if selectedOutbound != nil {
				if history := g.history.LoadURLTestHistory(RealTag(selectedOutbound, network)); history != nil {
					minOutbound = selectedOutbound
					minDelay = history.Delay
				}
			}
		}
	}
	outbounds := g.loadOutbounds()
	for _, detour := range outbounds {
		if !common.Contains(detour.Network(), network) {
			continue
		}
		history := g.history.LoadURLTestHistory(RealTag(detour, network))
		if history == nil {
			continue
		}
		if g.fallback.enabled && g.fallback.maxDelay > 0 && int64(history.Delay) > g.fallback.maxDelay {
			if fallbackIgnoreOutboundDelay == 0 || history.Delay < fallbackIgnoreOutboundDelay {
				fallbackIgnoreOutboundDelay = history.Delay
				fallbackIgnoreOutbound = detour
			}
			continue
		}
		if g.fallback.enabled {
			minDelay = history.Delay
			minOutbound = detour
			if minDelay == 0 {
				continue
			} else {
				break
			}
		}
		if minDelay == 0 || minDelay > history.Delay+g.tolerance {
			minDelay = history.Delay
			minOutbound = detour
		}
	}
	if minOutbound == nil && fallbackIgnoreOutbound != nil {
		return fallbackIgnoreOutbound, true
	}
	if minOutbound == nil {
		for _, detour := range outbounds {
			if !common.Contains(detour.Network(), network) {
				continue
			}
			return detour, false
		}
		return nil, false
	}
	return minOutbound, true
}

func (g *URLTestGroup) loopCheck(ticker *time.Ticker, closeChan <-chan struct{}) {
	if time.Since(g.lastActive.Load()) > g.interval {
		g.lastActive.Store(time.Now())
		g.CheckOutbounds(g.ctx, false)
	}
	for {
		select {
		case <-closeChan:
			return
		case <-ticker.C:
		}
		if time.Since(g.lastActive.Load()) > g.idleTimeout {
			g.access.Lock()
			if g.ticker == ticker {
				g.ticker.Stop()
				g.ticker = nil
				g.pause.UnregisterCallback(g.pauseCallback)
				g.pauseCallback = nil
			}
			g.access.Unlock()
			return
		}
		g.CheckOutbounds(g.ctx, false)
	}
}

func (g *URLTestGroup) CheckOutbounds(ctx context.Context, force bool) {
	_, _ = g.urlTest(ctx, force)
}

func (g *URLTestGroup) URLTest(ctx context.Context) (map[string]uint16, error) {
	return g.urlTest(ctx, true)
}

func (g *URLTestGroup) urlTest(ctx context.Context, force bool) (map[string]uint16, error) {
	if !g.checking.TryLock() {
		return make(map[string]uint16), nil
	}
	defer g.checking.Unlock()
	return g.urlTestLocked(ctx, force)
}

func (g *URLTestGroup) urlTestWait(ctx context.Context, force bool) (map[string]uint16, error) {
	g.checking.Lock()
	defer g.checking.Unlock()
	return g.urlTestLocked(ctx, force)
}

func (g *URLTestGroup) urlTestLocked(ctx context.Context, force bool) (map[string]uint16, error) {
	ctx = urltest.ContextWithUnifiedDelay(ctx, urltest.UnifiedDelayFromContext(g.ctx))
	ctx = urltest.ContextWithStatusMatcher(ctx, g.expectedStatus)
	result := URLTestOutbounds(ctx, g.outbound, g.history, g.logger, g.loadOutbounds(), g.link, g.interval, force)
	select {
	case <-ctx.Done():
	default:
		g.performUpdateCheck()
	}
	return result, nil
}

type urlTestResult struct {
	delay uint16
	err   error
}

type urlTestSessionKey struct {
	tag  string
	link string
}

type urlTestSessionCall struct {
	done   chan struct{}
	result urlTestResult
}

type urlTestSession struct {
	access sync.Mutex
	calls  map[urlTestSessionKey]*urlTestSessionCall
}

type urlTestSessionContextKey struct{}

type recursiveURLTestGroup interface {
	adapter.OutboundGroup
	urlTest(ctx context.Context, force bool) (map[string]uint16, error)
}

func urlTestSessionFromContext(ctx context.Context) (context.Context, *urlTestSession) {
	if session, loaded := ctx.Value(urlTestSessionContextKey{}).(*urlTestSession); loaded {
		return ctx, session
	}
	session := &urlTestSession{calls: make(map[urlTestSessionKey]*urlTestSessionCall)}
	return context.WithValue(ctx, urlTestSessionContextKey{}, session), session
}

func (s *urlTestSession) test(ctx context.Context, key urlTestSessionKey, test func() urlTestResult) (result urlTestResult) {
	s.access.Lock()
	call, loaded := s.calls[key]
	if !loaded {
		call = &urlTestSessionCall{done: make(chan struct{})}
		s.calls[key] = call
	}
	s.access.Unlock()
	if loaded {
		select {
		case <-call.done:
			return call.result
		case <-ctx.Done():
			return urlTestResult{err: ctx.Err()}
		}
	}
	defer func() {
		call.result = result
		close(call.done)
	}()
	return test()
}

type urlTestBatch struct {
	ctx      context.Context
	outbound adapter.OutboundManager
	history  *urltest.HistoryStorage
	logger   log.Logger
	session  *urlTestSession
	batch    *batch.Batch[any]
	checked  map[string]bool
	groups   []adapter.OutboundGroup
	access   sync.Mutex
	result   map[string]uint16
}

func URLTestOutbounds(ctx context.Context, outboundManager adapter.OutboundManager, history *urltest.HistoryStorage, logger log.Logger, outbounds []adapter.Outbound, link string, interval time.Duration, force bool) map[string]uint16 {
	ctx, session := urlTestSessionFromContext(ctx)
	b, _ := batch.New(ctx, batch.WithConcurrencyNum[any](10))
	testBatch := &urlTestBatch{
		ctx:      ctx,
		outbound: outboundManager,
		history:  history,
		logger:   logger,
		session:  session,
		batch:    b,
		checked:  make(map[string]bool),
		result:   make(map[string]uint16),
	}
	testBatch.test(outbounds, link, interval, force)
	b.Wait()
	for _, outboundGroup := range testBatch.groups {
		groupHistory := history.LoadURLTestHistory(RealTag(outboundGroup, N.NetworkTCP))
		if groupHistory != nil {
			testBatch.result[outboundGroup.Tag()] = groupHistory.Delay
		}
	}
	return testBatch.result
}

func (b *urlTestBatch) test(outbounds []adapter.Outbound, link string, interval time.Duration, force bool) {
	for _, detour := range outbounds {
		tag := detour.Tag()
		if b.checked[tag] {
			continue
		}
		switch nested := detour.(type) {
		case recursiveURLTestGroup:
			b.checked[tag] = true
			b.groups = append(b.groups, nested)
			b.batch.Go(tag, func() (any, error) {
				nestedResult, _ := nested.urlTest(b.ctx, force)
				b.access.Lock()
				maps.Copy(b.result, nestedResult)
				b.access.Unlock()
				return nil, nil
			})
		case adapter.OutboundGroup:
			b.checked[tag] = true
			b.groups = append(b.groups, nested)
			b.test(common.FilterNotNil(common.Map(nested.All(), func(it string) adapter.Outbound {
				member, _ := b.outbound.Outbound(it)
				return member
			})), link, interval, force)
		default:
			history := b.history.LoadURLTestHistory(tag)
			if !force && history != nil && time.Since(history.Time) < interval {
				continue
			}
			b.checked[tag] = true
			b.batch.Go(tag, func() (any, error) {
				testResult := b.session.test(b.ctx, urlTestSessionKey{tag: tag, link: link}, func() urlTestResult {
					testCtx, cancel := context.WithTimeout(b.ctx, C.TCPTimeout)
					defer cancel()
					testChan := make(chan urlTestResult, 1)
					go func() {
						delay, testErr := urltest.URLTest(testCtx, link, detour)
						testChan <- urlTestResult{delay, testErr}
					}()
					select {
					case testResult := <-testChan:
						return testResult
					case <-testCtx.Done():
						return urlTestResult{err: testCtx.Err()}
					}
				})
				if testResult.err != nil {
					if b.ctx.Err() != nil {
						return nil, nil
					}
					b.logger.Debug("outbound ", tag, " unavailable: ", testResult.err)
					b.history.DeleteURLTestHistory(tag)
				} else {
					b.logger.Debug("outbound ", tag, " available: ", testResult.delay, "ms")
					b.history.StoreURLTestHistory(tag, &adapter.URLTestHistory{
						Time:  time.Now(),
						Delay: testResult.delay,
					})
					b.access.Lock()
					b.result[tag] = testResult.delay
					b.access.Unlock()
				}
				return nil, nil
			})
		}
	}
}

func (g *URLTestGroup) performUpdateCheck() {
	g.updateAccess.Lock()
	defer g.updateAccess.Unlock()
	var (
		updated  bool
		selected bool
	)
	selectedOutboundTCP := g.selectedOutboundTCP.Load()
	if outbound, exists := g.Select(N.NetworkTCP); outbound != nil && (selectedOutboundTCP == nil || (exists && outbound != selectedOutboundTCP)) {
		if selectedOutboundTCP != nil {
			updated = true
		}
		g.selectedOutboundTCP.Store(outbound)
		selected = true
	}
	selectedOutboundUDP := g.selectedOutboundUDP.Load()
	if outbound, exists := g.Select(N.NetworkUDP); outbound != nil && (selectedOutboundUDP == nil || (exists && outbound != selectedOutboundUDP)) {
		if selectedOutboundUDP != nil {
			updated = true
		}
		g.selectedOutboundUDP.Store(outbound)
		selected = true
	}
	if updated {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
	}
	if selected {
		g.history.NotifyUpdated()
	}
}

func (g *URLTestGroup) loadOutbounds() []adapter.Outbound {
	g.outboundsAccess.RLock()
	defer g.outboundsAccess.RUnlock()
	return g.outbounds
}

func (g *URLTestGroup) storeOutbounds(outbounds []adapter.Outbound) {
	g.outboundsAccess.Lock()
	g.outbounds = outbounds
	g.outboundsAccess.Unlock()
}

func (g *URLTestGroup) replaceOutbounds(outbounds []adapter.Outbound) {
	g.updateAccess.Lock()
	selectedOutboundTCP := g.selectedOutboundTCP.Load()
	selectedOutboundUDP := g.selectedOutboundUDP.Load()
	g.storeOutbounds(outbounds)
	if !containsOutbound(outbounds, selectedOutboundTCP) {
		g.selectedOutboundTCP.Store(nil)
	}
	if !containsOutbound(outbounds, selectedOutboundUDP) {
		g.selectedOutboundUDP.Store(nil)
	}
	if g.selectedOutboundTCP.Load() == nil {
		if outbound, _ := g.Select(N.NetworkTCP); outbound != nil {
			g.selectedOutboundTCP.Store(outbound)
		}
	}
	if g.selectedOutboundUDP.Load() == nil {
		if outbound, _ := g.Select(N.NetworkUDP); outbound != nil {
			g.selectedOutboundUDP.Store(outbound)
		}
	}
	updated := (selectedOutboundTCP != nil && g.selectedOutboundTCP.Load() != selectedOutboundTCP) ||
		(selectedOutboundUDP != nil && g.selectedOutboundUDP.Load() != selectedOutboundUDP)
	selected := g.selectedOutboundTCP.Load() != selectedOutboundTCP || g.selectedOutboundUDP.Load() != selectedOutboundUDP
	g.updateAccess.Unlock()
	if updated {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
	}
	if selected && g.history != nil {
		g.history.NotifyUpdated()
	}
}

func containsOutbound(outbounds []adapter.Outbound, selected adapter.Outbound) bool {
	if selected == nil {
		return true
	}
	return slices.Contains(outbounds, selected)
}
