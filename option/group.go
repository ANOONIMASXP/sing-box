package option

import "github.com/sagernet/sing/common/json/badoption"

type SelectorOutboundOptions struct {
	GroupCommonOption
	Default                   string `json:"default,omitempty" reference:"outbound"`
	InterruptExistConnections bool   `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	GroupCommonOption
	URL                       string                 `json:"url,omitempty"`
	Interval                  badoption.Duration     `json:"interval,omitempty"`
	Tolerance                 uint16                 `json:"tolerance,omitempty"`
	IdleTimeout               badoption.Duration     `json:"idle_timeout,omitempty"`
	InterruptExistConnections bool                   `json:"interrupt_exist_connections,omitempty"`
	Fallback                  URLTestFallbackOptions `json:"fallback,omitempty"`
	// ExpectedStatus 对齐 mihomo/clash-meta: 控制 URL 探测通过�?HTTP 状态码�?
	// 支持语法: "204" / "200-299" / "200/204" / "200-299/301-302" / "*"�?
	// 为空时退回旧启发式（generate_204 必须 204，其�?link <400 即可）�?
	// 也接受下划线写法保持 sing-box snake_case 风格；mihomo 原生 kebab-case
	// 通过 "expected_status" �?"expected-status" �?JSON tag 兼容�?
	ExpectedStatus string `json:"expected_status,omitempty"`
}

type GroupCommonOption struct {
	Outbounds       []string          `json:"outbounds" reference:"outbound"`
	Providers       []string          `json:"providers" reference:"provider"`
	Exclude         *badoption.Regexp `json:"exclude,omitempty"`
	Include         *badoption.Regexp `json:"include,omitempty"`
	UseAllProviders bool              `json:"use_all_providers,omitempty"`

	// Hidden hints to dashboards / UI front-ends that this group should
	// not be displayed in the proxy switcher even though it remains
	// fully usable for routing rules. Exposed verbatim through Clash API
	// (GET /proxies) as the boolean `hidden` field �?clients decide
	// whether to honour it. mihomo-compatible.
	Hidden bool `json:"hidden,omitempty"`

	// Icon is an opaque string the dashboard renders alongside the
	// group name. Convention is a URL (https://...), data: URI, or a
	// short emoji �?sing-box does not interpret the value, it just
	// surfaces it through Clash API as the `icon` field. Empty string
	// means "no icon configured". mihomo-compatible.
	Icon string `json:"icon,omitempty"`
}

type URLTestFallbackOptions struct {
	Enabled  bool               `json:"enabled,omitempty"`
	MaxDelay badoption.Duration `json:"max_delay,omitempty"`
}

type LoadBalanceOutboundOptions struct {
	GroupCommonOption
	URL                       string             `json:"url,omitempty"`
	Interval                  badoption.Duration `json:"interval,omitempty"`
	IdleTimeout               badoption.Duration `json:"idle_timeout,omitempty"`
	TTL                       badoption.Duration `json:"ttl,omitempty"`
	InterruptExistConnections bool               `json:"interrupt_exist_connections,omitempty"`
	Strategy                  string             `json:"strategy,omitempty"`
	// ExpectedStatus �?URLTest；loadbalance 内部也做 URL 健康检查挑节点�?
	ExpectedStatus string `json:"expected_status,omitempty"`
}

