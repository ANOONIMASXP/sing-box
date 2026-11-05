package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/protocol/group"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
)

// newSmartHandler exposes the Smart engine and smart-loadbalance controls to
// the dashboard on the same origin as the API service. The native daemon API
// keeps the generic group model; these JSON endpoints carry the fork-specific
// diagnostics without forcing every client to regenerate its protobuf schema.
func newSmartHandler(ctx context.Context, secret string) http.Handler {
	handler := &smartHandler{ctx: ctx, secret: secret, mux: http.NewServeMux()}
	handler.mux.HandleFunc("GET /smart/groups", handler.handleGroups)
	handler.mux.HandleFunc("GET /smart/weights", handler.handleWeights)
	handler.mux.HandleFunc("GET /smart/groups/{tag}", handler.handleGroup)
	handler.mux.HandleFunc("POST /smart/groups/{tag}/recompute", handler.handleRecompute)
	handler.mux.HandleFunc("POST /smart/groups/{tag}/clear-selection", handler.handleClearSelection)
	handler.mux.HandleFunc("POST /smart/groups/{tag}/block/{node}", handler.handleBlock)
	handler.mux.HandleFunc("POST /smart/groups/{tag}/unblock/{node}", handler.handleUnblock)
	handler.mux.HandleFunc("GET /smart/groups/{tag}/regions", handler.handleRegions)
	handler.mux.HandleFunc("PUT /smart/groups/{tag}/regions", handler.handleUpdateRegions)
	handler.mux.HandleFunc("DELETE /smart/groups/{tag}/regions/lock", handler.handleUnlockRegion)
	handler.mux.HandleFunc("POST /smart/groups/{tag}/regions/detect", handler.handleDetectRegions)
	handler.mux.HandleFunc("PUT /smart/groups/{tag}/balance", handler.handleUpdateBalance)
	return handler
}

type smartHandler struct {
	ctx    context.Context
	secret string
	mux    *http.ServeMux
}

func (h *smartHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if h.secret != "" {
		expected := []byte("Bearer " + h.secret)
		actual := []byte(request.Header.Get("Authorization"))
		if len(expected) != len(actual) || subtle.ConstantTimeCompare(expected, actual) != 1 {
			writeSmartError(writer, http.StatusUnauthorized, "unauthorized")
			return
		}
	}
	h.mux.ServeHTTP(writer, request)
}

func (h *smartHandler) smartGroups() ([]*group.Smart, error) {
	outboundManager := service.FromContext[adapter.OutboundManager](h.ctx)
	if outboundManager == nil {
		return nil, E.New("outbound manager unavailable")
	}
	var groups []*group.Smart
	for _, outbound := range outboundManager.Outbounds() {
		if smartGroup, loaded := outbound.(*group.Smart); loaded {
			groups = append(groups, smartGroup)
		}
	}
	return groups, nil
}

func (h *smartHandler) smartGroup(tag string) (*group.Smart, error) {
	groups, err := h.smartGroups()
	if err != nil {
		return nil, err
	}
	for _, smartGroup := range groups {
		if smartGroup.Tag() == tag {
			return smartGroup, nil
		}
	}
	return nil, E.New("smart group not found: ", tag)
}

func smartSummary(smartGroup *group.Smart) map[string]any {
	summary := map[string]any{
		"name":           smartGroup.Tag(),
		"type":           smartGroup.Type(),
		"testUrl":        smartGroup.TestURL(),
		"expectedStatus": smartGroup.ExpectedStatus(),
		"preferAsn":      smartGroup.PreferASN(),
		"useLightGBM":    smartGroup.UseLightGBM(),
		"collectData":    smartGroup.CollectData(),
		"sampleRate":     smartGroup.SampleRate(),
		"tolerance":      smartGroup.Tolerance(),
		"fixed":          smartGroup.FixedSelection(),
		"now":            smartGroup.Now(),
		"members":        len(smartGroup.MemberTags()),
		"loadBalance":    smartGroup.IsLoadBalance(),
	}
	if rules := smartGroup.PolicyPriority(); len(rules) > 0 {
		priority := make([]map[string]any, len(rules))
		for index, rule := range rules {
			priority[index] = map[string]any{"pattern": rule.Pattern, "factor": rule.Factor}
		}
		summary["policyPriority"] = priority
	}
	if age := smartGroup.LGBMModelAge(); age > 0 {
		summary["lgbmModelAge"] = age.Round(time.Second).String()
	}
	if smartGroup.IsLoadBalance() {
		summary["balanceInfo"] = smartGroup.BalanceInfo()
	}
	return summary
}

func (h *smartHandler) handleGroups(writer http.ResponseWriter, request *http.Request) {
	groups, err := h.smartGroups()
	if err != nil {
		writeSmartError(writer, http.StatusServiceUnavailable, err.Error())
		return
	}
	summaries := make([]map[string]any, 0, len(groups))
	for _, smartGroup := range groups {
		summaries = append(summaries, smartSummary(smartGroup))
	}
	writeSmartJSON(writer, http.StatusOK, map[string]any{"groups": summaries})
}

func (h *smartHandler) handleWeights(writer http.ResponseWriter, request *http.Request) {
	groups, err := h.smartGroups()
	if err != nil {
		writeSmartError(writer, http.StatusServiceUnavailable, err.Error())
		return
	}
	weights := make(map[string]any, len(groups))
	for _, smartGroup := range groups {
		ranking, err := smartGroup.WeightRanking(false)
		if err == nil {
			weights[smartGroup.Tag()] = ranking
		}
	}
	writeSmartJSON(writer, http.StatusOK, map[string]any{"weights": weights})
}

func (h *smartHandler) handleGroup(writer http.ResponseWriter, request *http.Request) {
	smartGroup, err := h.smartGroup(request.PathValue("tag"))
	if err != nil {
		writeSmartError(writer, http.StatusNotFound, err.Error())
		return
	}
	detail := smartSummary(smartGroup)
	for key, value := range smartGroup.Diagnostic() {
		detail[key] = value
	}
	if ranking, err := smartGroup.WeightRanking(false); err == nil {
		detail["ranking"] = ranking
	} else {
		detail["rankingError"] = err.Error()
	}
	if smartGroup.IsLoadBalance() {
		detail["regionTable"] = smartGroup.RegionTable()
		detail["balanceInfo"] = smartGroup.BalanceInfo()
	}
	writeSmartJSON(writer, http.StatusOK, detail)
}

func (h *smartHandler) handleRecompute(writer http.ResponseWriter, request *http.Request) {
	smartGroup, err := h.smartGroup(request.PathValue("tag"))
	if err != nil {
		writeSmartError(writer, http.StatusNotFound, err.Error())
		return
	}
	ranking, err := smartGroup.WeightRanking(true)
	if err != nil {
		writeSmartError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	writeSmartJSON(writer, http.StatusOK, map[string]any{"group": smartGroup.Tag(), "ranking": ranking})
}

func (h *smartHandler) handleClearSelection(writer http.ResponseWriter, request *http.Request) {
	smartGroup, err := h.smartGroup(request.PathValue("tag"))
	if err != nil {
		writeSmartError(writer, http.StatusNotFound, err.Error())
		return
	}
	writeSmartJSON(writer, http.StatusOK, smartGroup.ClearSelection())
}

func (h *smartHandler) handleBlock(writer http.ResponseWriter, request *http.Request) {
	smartGroup, err := h.smartGroup(request.PathValue("tag"))
	if err != nil {
		writeSmartError(writer, http.StatusNotFound, err.Error())
		return
	}
	node := request.PathValue("node")
	duration := group.DefaultBlockDuration
	if value := request.URL.Query().Get("duration"); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 {
			duration = parsed
		}
	}
	if err = smartGroup.MarkBlocked(node, duration); err != nil {
		writeSmartError(writer, http.StatusNotFound, err.Error())
		return
	}
	writeSmartJSON(writer, http.StatusOK, map[string]any{
		"group":        smartGroup.Tag(),
		"node":         node,
		"blockedUntil": time.Now().Add(duration).Format(time.RFC3339),
	})
}

func (h *smartHandler) handleUnblock(writer http.ResponseWriter, request *http.Request) {
	smartGroup, err := h.smartGroup(request.PathValue("tag"))
	if err != nil {
		writeSmartError(writer, http.StatusNotFound, err.Error())
		return
	}
	node := request.PathValue("node")
	smartGroup.UnblockNode(node)
	writeSmartJSON(writer, http.StatusOK, map[string]any{"group": smartGroup.Tag(), "node": node})
}

func (h *smartHandler) loadBalanceGroup(request *http.Request) (*group.Smart, error) {
	smartGroup, err := h.smartGroup(request.PathValue("tag"))
	if err != nil {
		return nil, err
	}
	if !smartGroup.IsLoadBalance() {
		return nil, E.New("not a smart-loadbalance group: ", smartGroup.Tag())
	}
	return smartGroup, nil
}

func (h *smartHandler) handleRegions(writer http.ResponseWriter, request *http.Request) {
	smartGroup, err := h.loadBalanceGroup(request)
	if err != nil {
		writeSmartError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeSmartJSON(writer, http.StatusOK, smartGroup.RegionTable())
}

func (h *smartHandler) handleUpdateRegions(writer http.ResponseWriter, request *http.Request) {
	smartGroup, err := h.loadBalanceGroup(request)
	if err != nil {
		writeSmartError(writer, http.StatusBadRequest, err.Error())
		return
	}
	var body struct {
		Lock *string `json:"lock"`
		Mode *string `json:"mode"`
	}
	if err = json.NewDecoder(request.Body).Decode(&body); err != nil {
		writeSmartError(writer, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if body.Lock == nil && body.Mode == nil {
		writeSmartError(writer, http.StatusBadRequest, `body must set "lock" and / or "mode"`)
		return
	}
	response := map[string]any{"group": smartGroup.Tag()}
	if body.Mode != nil {
		mode, err := smartGroup.SetRegionMode(*body.Mode)
		if err != nil {
			writeSmartError(writer, http.StatusBadRequest, err.Error())
			return
		}
		response["mode"] = mode
	}
	if body.Lock != nil {
		lock, err := smartGroup.SetRegionLock(*body.Lock)
		if err != nil {
			writeSmartError(writer, http.StatusBadRequest, err.Error())
			return
		}
		response["lock"] = lock
	}
	response["now"] = smartGroup.Now()
	writeSmartJSON(writer, http.StatusOK, response)
}

func (h *smartHandler) handleUnlockRegion(writer http.ResponseWriter, request *http.Request) {
	smartGroup, err := h.loadBalanceGroup(request)
	if err != nil {
		writeSmartError(writer, http.StatusBadRequest, err.Error())
		return
	}
	previous := smartGroup.RegionLock()
	if _, err = smartGroup.SetRegionLock(""); err != nil {
		writeSmartError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeSmartJSON(writer, http.StatusOK, map[string]any{
		"group":    smartGroup.Tag(),
		"previous": previous,
		"now":      smartGroup.Now(),
	})
}

func (h *smartHandler) handleDetectRegions(writer http.ResponseWriter, request *http.Request) {
	smartGroup, err := h.loadBalanceGroup(request)
	if err != nil {
		writeSmartError(writer, http.StatusBadRequest, err.Error())
		return
	}
	force := request.URL.Query().Get("force") == "true" || request.URL.Query().Get("force") == "1"
	started := smartGroup.RedetectRegions(force)
	writeSmartJSON(writer, http.StatusOK, map[string]any{"group": smartGroup.Tag(), "exitProbes": started})
}

func (h *smartHandler) handleUpdateBalance(writer http.ResponseWriter, request *http.Request) {
	smartGroup, err := h.loadBalanceGroup(request)
	if err != nil {
		writeSmartError(writer, http.StatusBadRequest, err.Error())
		return
	}
	var body struct {
		Strategy string `json:"strategy"`
		Affinity string `json:"affinity"`
	}
	if err = json.NewDecoder(request.Body).Decode(&body); err != nil {
		writeSmartError(writer, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	strategy, affinity, err := smartGroup.SetBalance(body.Strategy, body.Affinity)
	if err != nil {
		writeSmartError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeSmartJSON(writer, http.StatusOK, map[string]any{
		"group":    smartGroup.Tag(),
		"strategy": strategy,
		"affinity": affinity,
	})
}

func writeSmartJSON(writer http.ResponseWriter, statusCode int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeSmartError(writer http.ResponseWriter, statusCode int, message string) {
	writeSmartJSON(writer, statusCode, map[string]any{"error": strings.TrimSpace(message)})
}
