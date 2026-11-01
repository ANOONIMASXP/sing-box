### Structure

```json
{
  "type": "urltest",
  "tag": "auto",
  
  "outbounds": [
    "proxy-a",
    "proxy-b",
    "proxy-c"
  ],
  "providers": [
    "provider-a",
    "provider-b",
  ],
  "exclude": "",
  "include": "",
  "include_type": [],
  "exclude_type": [],
  "include_transport": [],
  "exclude_transport": [],
  "network": "",
  "url": "",
  "interval": "",
  "tolerance": 50,
  "idle_timeout": "",
  "use_all_providers": false,
  "fallback": {
    "enabled": false,
    "max_delay": ""
  },
  "interrupt_exist_connections": false
}
```

### Fields

#### outbounds

List of outbound tags to test.

#### providers

List of [Provider](/configuration/provider) tags to test.

#### exclude

Exclude regular expression to filter `providers` nodes.

#### include

Include regular expression to filter `providers` nodes.

#### include_type

List of outbound types to keep among `providers` nodes, such as `vless` or `hysteria2`.

Case-insensitive. Every outbound and endpoint type of the build is accepted, together with the Clash names and short forms `ss`, `socks5`, `hy`, `hy1`, `hy2`, `wg`, `naiveproxy`, `shadow-tls`, `openvpn` and `masque`. Unknown types are rejected at startup.

#### exclude_type

List of outbound types to drop from `providers` nodes. Takes precedence over `include_type`.

#### include_transport

List of transports to keep among `providers` nodes: `tcp` (no transport), `ws`, `httpupgrade`, `http`, `grpc`, `quic`, `xhttp` and other registered plugin transports. The Clash names `websocket`, `h2`, `gun`, `raw` and `splithttp` are accepted.

Only `vmess`, `vless`, `trojan` and `shadowsocks` (whose `v2ray-plugin` counts as `ws` or `quic`) have a transport. Other nodes match neither list: `include_transport` drops them and `exclude_transport` keeps them.

#### exclude_transport

List of transports to drop from `providers` nodes. Takes precedence over `include_transport`.

#### network

`tcp`, `udp`, or a list of both.

Keep only `providers` nodes that support every listed network. The capability comes from the node itself, for example `"network": "tcp"` on a Shadowsocks node or `udp: false` in a Clash subscription, not from its name. When the provider probes UDP (`health_check.udp`), a node whose valid probe failed is removed until a probe succeeds again.

How each protocol reports UDP:

| Protocol | UDP |
|---|---|
| `shadowsocks` `vmess` `vless` `trojan` `socks` `snell` `anytls` `quicx` | from its `network` field, the Clash `udp` flag for subscriptions |
| `hysteria` `hysteria2` `tuic` | yes, unless `network` is `tcp`; always yes from Clash subscriptions, as in Clash |
| `socks` version `4` / `4a`, `naive` | only with `udp_over_tcp` |
| `http` | from its `network` field (CONNECT-UDP); always no from Clash subscriptions |
| `ssh` `shadowtls` `tor` | no |
| `wireguard` `tailscale` `tailcat` `nowhere` and other endpoints | yes |

!!! note ""

    `include`, `exclude`, `include_type`, `exclude_type`, `include_transport`, `exclude_transport` and `network` behave the same in selector, urltest, loadbalance and smart groups, are applied again on every provider update, and all apply to `providers` nodes only, and a node must pass every one of them. Members listed in `outbounds` are always kept.

#### url

The URL to test. `https://www.gstatic.com/generate_204` will be used if empty.

#### interval

The test interval. `3m` will be used if empty.

Every interval, members whose last result is older than the interval are tested, up to 10 at a time.
Members of nested groups are tested through those groups, and a member reachable through several of them is tested once per round.
Health checks run only while the group is in use; after `idle_timeout` without connections they pause until the next connection.
A network change tests every member again.

#### tolerance

The test tolerance in milliseconds. `50` will be used if empty.

#### idle_timeout

The idle timeout. `30m` will be used if empty.

#### use_all_providers

Whether to use all providers for testing. `false` will be used if empty.

#### fallback

Fallback selection configuration.

When enabled, the first available outbound is selected in configuration order instead of selecting the outbound with the lowest delay.

##### fallback.enabled

Enable fallback selection.

##### fallback.max_delay

Maximum acceptable delay.

An outbound whose delay exceeds this value is skipped. If every available outbound exceeds the value, the skipped outbound with the lowest delay is selected.

#### interrupt_exist_connections

Interrupt existing connections when the selected outbound has changed.

Only inbound connections are affected by this setting, internal connections will always be interrupted.

### Health check behavior

* **Selection**: the fastest member is found first; the current member is kept while its delay is within `tolerance` of it.
* **Cold start**: until members are measured, a random member supporting the network is picked and kept, so the first connections are not all sent to the first member.
* **Startup**: when no member is available after startup (the network is not ready yet), members are tested 15s, 35s and 60s after startup, never later than `interval`, until a member answers or the group goes idle.
* **Failed connections**: when a connection through the group fails, the failed member is tested alone 3s later. If it answers, it keeps its delay and is trusted for one minute. Otherwise its result is dropped and the fastest other member is selected; when no member is left alive, another member is picked at random and every member is tested again, and for the next 90s further failures switch members right away.
* **Manual selection**: selecting a member through the Clash API or the client tests it first and is rejected when it does not answer. The member is then pinned: health checks and failed connections never move it, and it is restored after a restart. While pinned, health checks only test the pinned member. The pin is released by a group speed test or by selecting an empty name (`DELETE /proxies/{group}` in the Clash API), which returns to the fastest member at once and refreshes the others in the background. A pin whose member disappears from the providers is released; a TCP-only pin leaves UDP on automatic selection.
