package urltest

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sync"
	"time"

	"github.com/sagernet/sing-anytls"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/smart/tcpinfo"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-mux"
	"github.com/sagernet/sing-snell"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/ntp"
	"github.com/sagernet/sing/common/observable"
)

type unifiedDelayKey struct{}

// ContextWithUnifiedDelay binds the measurement policy to one instance or request.
func ContextWithUnifiedDelay(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, unifiedDelayKey{}, enabled)
}

func UnifiedDelayFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	enabled, _ := ctx.Value(unifiedDelayKey{}).(bool)
	return enabled
}

type statusMatcherKey struct{}

// ContextWithStatusMatcher propagates the expected-status matcher through
// probe chains that cannot thread an explicit parameter (recursive
// URLTestOutbounds, daemon RPCs, ...). An explicit matcher argument on
// URLTestWithStatus / URLTestWithDetailAndStatus always wins over the
// context value.
func ContextWithStatusMatcher(ctx context.Context, matcher *StatusMatcher) context.Context {
	if matcher == nil {
		return ctx
	}
	return context.WithValue(ctx, statusMatcherKey{}, matcher)
}

// StatusMatcherFromContext returns the expected-status matcher bound to ctx,
// or nil when no group configured one (legacy behaviour: accept any status).
func StatusMatcherFromContext(ctx context.Context) *StatusMatcher {
	if ctx == nil {
		return nil
	}
	matcher, _ := ctx.Value(statusMatcherKey{}).(*StatusMatcher)
	return matcher
}

// URLTestDetail carries per-phase timings and kernel counters collected
// during a single probe. Fields the platform / dialer cannot observe
// (DNS via remote proxy, TCP counters on non-Linux or behind wrapped
// connections) stay zero.
type URLTestDetail struct {
	TCPConnectMS   int64
	TLSHandshakeMS int64
	FirstByteMS    int64
	DidResume      bool
	DNSResolveMS   int64

	TCPRetransmissions uint32
	TCPLosses          uint32
	PathMTU            uint32
}

type HistoryStorage struct {
	access       sync.RWMutex
	delayHistory map[string]*adapter.URLTestHistory
	updateHooks  []*observable.Subscriber[struct{}]
}

func NewHistoryStorage() *HistoryStorage {
	return &HistoryStorage{
		delayHistory: make(map[string]*adapter.URLTestHistory),
	}
}

func (s *HistoryStorage) AddUpdateHook(hook *observable.Subscriber[struct{}]) {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = append(s.updateHooks, hook)
}

func (s *HistoryStorage) NotifyUpdated() {
	s.access.RLock()
	defer s.access.RUnlock()
	s.notifyUpdated()
}

func (s *HistoryStorage) LoadURLTestHistory(tag string) *adapter.URLTestHistory {
	if s == nil {
		return nil
	}
	s.access.RLock()
	defer s.access.RUnlock()
	return s.delayHistory[tag]
}

func (s *HistoryStorage) DeleteURLTestHistory(tag string) {
	s.access.Lock()
	delete(s.delayHistory, tag)
	s.notifyUpdated()
	s.access.Unlock()
}

func (s *HistoryStorage) StoreURLTestHistory(tag string, history *adapter.URLTestHistory) {
	s.access.Lock()
	s.delayHistory[tag] = history
	s.notifyUpdated()
	s.access.Unlock()
}

func (s *HistoryStorage) notifyUpdated() {
	for _, updateHook := range s.updateHooks {
		updateHook.Emit(struct{}{})
	}
}

func (s *HistoryStorage) Close() error {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = nil
	return nil
}

func URLTest(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
	return URLTestWithDetailAndStatus(ctx, link, detour, nil, nil)
}

// URLTestWithStatus probes link and additionally validates the response
// status code against matcher. A nil matcher keeps the legacy behaviour
// (any HTTP response counts as success) unless ctx carries one via
// ContextWithStatusMatcher.
func URLTestWithStatus(ctx context.Context, link string, detour N.Dialer, matcher *StatusMatcher) (uint16, error) {
	return URLTestWithDetailAndStatus(ctx, link, detour, nil, matcher)
}

// URLTestWithDetail is URLTest but additionally fills per-phase timings
// and kernel counters into detail for callers that feed ML features.
func URLTestWithDetail(ctx context.Context, link string, detour N.Dialer, detail *URLTestDetail) (uint16, error) {
	return URLTestWithDetailAndStatus(ctx, link, detour, detail, nil)
}

// URLTestWithDetailAndStatus is the full probe entry: detail and matcher
// may both be nil.
func URLTestWithDetailAndStatus(ctx context.Context, link string, detour N.Dialer, detail *URLTestDetail, matcher *StatusMatcher) (uint16, error) {
	multiplexOutbound, isMultiplexOutbound := common.Cast[adapter.OutboundWithMultiplex](detour)
	if isMultiplexOutbound && multiplexOutbound.MultiplexEnabled() {
		warmContext := adapter.ContextWithKeepSession(ctx)
		warmContext = mux.ContextWithKeepSession(warmContext)
		warmContext = anytls.ContextWithKeepSession(warmContext)
		warmContext = contextWithQUICKeepSession(warmContext)
		warmContext = snell.ContextWithKeepSession(warmContext)
		_, err := urlTest(warmContext, link, detour, nil, matcher)
		if err != nil {
			return 0, err
		}
	}
	return urlTest(ctx, link, detour, detail, matcher)
}

func urlTest(ctx context.Context, link string, detour N.Dialer, detail *URLTestDetail, matcher *StatusMatcher) (t uint16, err error) {
	if matcher == nil {
		matcher = StatusMatcherFromContext(ctx)
	}
	if link == "" {
		link = "https://www.gstatic.com/generate_204"
	}
	linkURL, err := url.Parse(link)
	if err != nil {
		return
	}
	hostname := linkURL.Hostname()
	port := linkURL.Port()
	if port == "" {
		switch linkURL.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}

	start := time.Now()
	instance, err := detour.DialContext(ctx, "tcp", M.ParseSocksaddrHostPortStr(hostname, port))
	if err != nil {
		return
	}
	defer instance.Close()
	if detail != nil {
		detail.TCPConnectMS = time.Since(start).Milliseconds()
	}
	if N.NeedHandshakeForWrite(instance) {
		start = time.Now()
	}
	var (
		tlsStart    time.Time
		tlsDone     time.Time
		firstByteAt time.Time
	)
	if detail != nil {
		trace := &httptrace.ClientTrace{
			TLSHandshakeStart: func() { tlsStart = time.Now() },
			TLSHandshakeDone: func(_ tls.ConnectionState, _ error) { tlsDone = time.Now() },
			GotFirstResponseByte: func() { firstByteAt = time.Now() },
		}
		ctx = httptrace.WithClientTrace(ctx, trace)
	}
	req, err := http.NewRequest(http.MethodHead, link, nil)
	if err != nil {
		return
	}
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return instance, nil
			},
			TLSClientConfig: &tls.Config{
				Time:    ntp.TimeFuncFromContext(ctx),
				RootCAs: adapter.RootPoolFromContext(ctx),
			},
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: C.TCPTimeout,
	}
	defer client.CloseIdleConnections()
	reqStart := time.Now()
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return
	}
	resp.Body.Close()
	elapsed := time.Since(start)
	if UnifiedDelayFromContext(ctx) {
		second := time.Now()
		var ignoredErr error
		var secondResp *http.Response
		secondResp, ignoredErr = client.Do(req.WithContext(ctx))
		if ignoredErr == nil {
			resp = secondResp
			resp.Body.Close()
			elapsed = time.Since(second)
		}
	}
	if detail != nil {
		if !tlsStart.IsZero() && !tlsDone.IsZero() {
			detail.TLSHandshakeMS = tlsDone.Sub(tlsStart).Milliseconds()
		}
		if !firstByteAt.IsZero() {
			detail.FirstByteMS = firstByteAt.Sub(reqStart).Milliseconds()
		}
		if resp.TLS != nil {
			detail.DidResume = resp.TLS.DidResume
		}
		// Kernel TCP counters before the deferred Close invalidates the fd.
		// tcpinfo.Read returns ok=false for proxy wrappers that hide the
		// underlying fd (most of them) and on non-Linux.
		if tinfo, ok := tcpinfo.Read(instance); ok {
			detail.TCPRetransmissions = tinfo.Retransmissions
			detail.TCPLosses = tinfo.Losses
			detail.PathMTU = tinfo.PathMTU
		}
	}
	if matcher != nil && !matcher.Match(resp.StatusCode) {
		return 0, fmt.Errorf("urltest: unexpected HTTP status %d", resp.StatusCode)
	}
	t = uint16(elapsed / time.Millisecond)
	return
}
