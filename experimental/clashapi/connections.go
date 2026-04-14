package clashapi

import (
	"bytes"
	"context"
	"net/http"
	"net/netip"
	"strconv"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/trafficcontrol"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing/common"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/ws"
	"github.com/sagernet/ws/wsutil"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/gofrs/uuid/v5"
)

func connectionRouter(ctx context.Context, network adapter.NetworkManager, trafficManager *trafficcontrol.Manager) http.Handler {
	r := chi.NewRouter()
	r.Get("/", getConnections(ctx, trafficManager))
	r.Delete("/", closeAllConnections(ctx, network, trafficManager))
	r.Delete("/{id}", closeConnection(ctx, trafficManager))
	// Smart-block: close the connection AND mark its upstream Smart-selected
	// node as blocked so the group stops selecting it for a cooldown window.
	// Mirrors mihomo's `DELETE /connections/smart/{id}`.
	r.Delete("/smart/{id}", smartBlockConnection(ctx, trafficManager))
	return r
}

func connectionsSnapshot(trafficManager *trafficcontrol.Manager) render.M {
	uplinkTotal, downlinkTotal := trafficManager.Total()
	connections := common.Filter(trafficManager.Connections(), func(metadata *trafficcontrol.TrackerMetadata) bool {
		return metadata.OutboundType != C.TypeDNS
	})
	return render.M{
		"downloadTotal": downlinkTotal,
		"uploadTotal":   uplinkTotal,
		"connections": common.Map(connections, func(metadata *trafficcontrol.TrackerMetadata) connectionObject {
			return connectionObject(*metadata)
		}),
		"memory": inuseMemory(),
	}
}

type connectionObject trafficcontrol.TrackerMetadata

func (c connectionObject) MarshalJSON() ([]byte, error) {
	var inbound string
	if c.Metadata.Inbound != "" {
		inbound = c.Metadata.InboundType + "/" + c.Metadata.Inbound
	} else {
		inbound = c.Metadata.InboundType
	}
	var domain string
	if c.Metadata.Destination.Fqdn != "" {
		domain = c.Metadata.Destination.Fqdn
	} else {
		domain = c.Metadata.Domain
	}
	var destinationAddr netip.Addr
	if len(c.Metadata.DestinationAddresses) > 0 {
		destinationAddr = c.Metadata.DestinationAddresses[0]
	} else {
		destinationAddr = c.Metadata.Destination.Addr
	}
	var processPath string
	if c.Metadata.ProcessInfo != nil {
		if len(c.Metadata.ProcessInfo.PackageNames) > 0 {
			processPath = c.Metadata.ProcessInfo.PackageNames[0]
		} else if len(c.Metadata.ProcessInfo.ProcessPaths) > 0 {
			processPath = c.Metadata.ProcessInfo.ProcessPaths[0]
		}
		if processPath == "" {
			if c.Metadata.ProcessInfo.UserId != -1 {
				processPath = F.ToString(c.Metadata.ProcessInfo.UserId)
			}
		} else if c.Metadata.ProcessInfo.UserName != "" {
			processPath = F.ToString(processPath, " (", c.Metadata.ProcessInfo.UserName, ")")
		} else if c.Metadata.ProcessInfo.UserId != -1 {
			processPath = F.ToString(processPath, " (", c.Metadata.ProcessInfo.UserId, ")")
		}
	}
	var rule string
	if c.Rule != nil {
		rule = F.ToString(c.Rule, " => ", c.Rule.Action())
	} else {
		rule = "final"
	}
	return json.Marshal(map[string]any{
		"id": c.ID,
		"metadata": map[string]any{
			"network":         c.Metadata.Network,
			"type":            inbound,
			"sourceIP":        c.Metadata.Source.Addr,
			"destinationIP":   destinationAddr,
			"sourcePort":      F.ToString(c.Metadata.Source.Port),
			"destinationPort": F.ToString(c.Metadata.Destination.Port),
			"host":            domain,
			"sniffHost":       c.Metadata.SniffHost,
			"dnsMode":         "normal",
			"processPath":     processPath,
		},
		"upload":      c.Upload.Load(),
		"download":    c.Download.Load(),
		"start":       c.CreatedAt,
		"chains":      c.Chain,
		"rule":        rule,
		"rulePayload": "",
	})
}

// notifySmartUserDisconnect walks the tracker's real-outbound chain to find
// a Smart group; if one is found, calls Smart.NotifyUserDisconnect with the
// downstream node + target so manual close-via-API events participate in
// short-life → markDead escalation identically to direct smartTrackedConn.Close.
//
// Called IMMEDIATELY BEFORE tracker.Close() so the Smart state updates
// before the async recordStats goroutine in smartTrackedConn.Close fires.
// That race used to let the user's next DialContext hit the same stale
// unwrap cache even when closing explicitly through the API.
func notifySmartUserDisconnect(ctx context.Context, meta *trafficcontrol.TrackerMetadata) {
	if meta == nil {
		return
	}
	outboundMgr := service.FromContext[adapter.OutboundManager](ctx)
	if outboundMgr == nil {
		return
	}
	chain := meta.Metadata.GetRealOutboundChain()
	for i, tag := range chain {
		ob, ok := outboundMgr.Outbound(tag)
		if !ok {
			continue
		}
		sg, ok := ob.(*group.Smart)
		if !ok {
			continue
		}
		nodeTag := ""
		if i+1 < len(chain) {
			nodeTag = chain[i+1]
		}
		if nodeTag == "" {
			nodeTag = sg.Now()
		}
		if nodeTag == "" {
			return
		}
		// Extract target + isUDP from metadata for the short-life key.
		target := ""
		if meta.Metadata.Destination.Fqdn != "" {
			target = meta.Metadata.Destination.Fqdn
		} else if meta.Metadata.SniffHost != "" {
			target = meta.Metadata.SniffHost
		}
		isUDP := meta.Metadata.Network == "udp"
		sg.NotifyUserDisconnect(target, nodeTag, isUDP, "")
		return
	}
}

func getConnections(ctx context.Context, trafficManager *trafficcontrol.Manager) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "websocket" {
			render.JSON(w, r, connectionsSnapshot(trafficManager))
			return
		}

		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()

		intervalStr := r.URL.Query().Get("interval")
		interval := 1000
		if intervalStr != "" {
			t, err := strconv.Atoi(intervalStr)
			if err != nil {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, ErrBadRequest)
				return
			}

			interval = t
		}

		buf := &bytes.Buffer{}
		sendSnapshot := func() error {
			buf.Reset()
			encodeErr := json.NewEncoder(buf).Encode(connectionsSnapshot(trafficManager))
			if encodeErr != nil {
				return encodeErr
			}
			return wsutil.WriteServerText(conn, buf.Bytes())
		}

		if err = sendSnapshot(); err != nil {
			return
		}

		tick := time.NewTicker(time.Millisecond * time.Duration(interval))
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if err = sendSnapshot(); err != nil {
				break
			}
		}
	}
}

func closeConnection(ctx context.Context, trafficManager *trafficcontrol.Manager) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		id := uuid.FromStringOrNil(chi.URLParam(r, "id"))
		targetConnection := trafficManager.Connection(id)
		if targetConnection != nil {
			// Notify any Smart group in the chain BEFORE closing so the
			// short-life aggregator receives an explicit user-initiated
			// disconnect signal. Without this, an API-only close was
			// indistinguishable from a normal server FIN.
			notifySmartUserDisconnect(ctx, targetConnection.Metadata())
			targetConnection.Close()
		}
		render.NoContent(w, r)
	}
}

func closeAllConnections(ctx context.Context, network adapter.NetworkManager, trafficManager *trafficcontrol.Manager) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		for _, metadata := range trafficManager.Connections() {
			// Every conn in the bulk close is user-initiated; feed Smart so
			// a "close all" click also contributes to short-life counters.
			notifySmartUserDisconnect(ctx, metadata)
		}
		trafficManager.CloseAllConnections()
		network.ResetNetwork(ctx)
		render.NoContent(w, r)
	}
}

// smartBlockConnection closes the connection identified by id and, if it was
// routed through a Smart group, additionally marks the selected node as
// blocked within that group. Looks up the group and its downstream node via
// the connection's RealOutboundChain.
func smartBlockConnection(ctx context.Context, trafficManager *trafficcontrol.Manager) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		id := uuid.FromStringOrNil(chi.URLParam(r, "id"))
		if id == uuid.Nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}
		snapshot := trafficManager.Snapshot()
		var target *trafficcontrol.TrackerMetadata
		for _, c := range snapshot.Connections {
			meta := c.Metadata()
			if meta != nil && meta.ID == id {
				target = meta
				c.Close()
				break
			}
		}
		if target == nil {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, ErrNotFound)
			return
		}

		// Walk the chain looking for a Smart group. The slot immediately after
		// the Smart tag in the chain is the actual node it selected.
		chain := target.Metadata.GetRealOutboundChain()
		outboundMgr := service.FromContext[adapter.OutboundManager](ctx)
		if outboundMgr == nil {
			render.NoContent(w, r)
			return
		}
		var blocked struct {
			Group string `json:"group,omitempty"`
			Node  string `json:"node,omitempty"`
		}
		for i, tag := range chain {
			ob, ok := outboundMgr.Outbound(tag)
			if !ok {
				continue
			}
			sg, ok := ob.(*group.Smart)
			if !ok {
				continue
			}
			nodeTag := ""
			if i+1 < len(chain) {
				nodeTag = chain[i+1]
			}
			if nodeTag == "" {
				nodeTag = sg.Now()
			}
			if nodeTag == "" {
				break
			}
			if err := sg.MarkBlocked(nodeTag, group.DefaultBlockDuration); err == nil {
				blocked.Group = tag
				blocked.Node = nodeTag
			}
			break
		}

		render.JSON(w, r, blocked)
	}
}
