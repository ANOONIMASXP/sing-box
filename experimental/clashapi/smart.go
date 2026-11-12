// Smart Clash API endpoints. mihomo's own routes (GET /group/{name}/weights,
// GET /group/weights, POST /cache/smart/flush, DELETE
// /connections/smart/{id}) live with their routers; this file hosts the
// /smart extensions: group listing, diagnostics with the comprehensive
// score state, node blocks and the smart-loadbalance region controls.
package clashapi

import (
	"context"
	"net/http"
	"time"

	"github.com/sagernet/sing-box/experimental/smartapi"
	"github.com/sagernet/sing-box/protocol/group"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

// smartRouter mounts at /smart. It is kept for Clash API compatibility;
// native clients should use the StartedService Smart RPCs.
func smartRouter(ctx context.Context) http.Handler {
	r := chi.NewRouter()
	r.Get("/weights", getAllSmartWeights(ctx))
	r.Get("/groups", listSmartGroups(ctx))
	r.Get("/groups/{name}/diag", smartGroupDiag(ctx))
	r.Post("/groups/{name}/block/{node}", blockSmartNode(ctx))
	r.Delete("/groups/{name}/block/{node}", unblockSmartNode(ctx))
	r.Post("/groups/{name}/recompute", recomputeSmartWeights(ctx))
	r.Post("/groups/{name}/clear-selection", clearSmartSelection(ctx))
	// smart-loadbalance: region table, region lock / mode, balance
	// strategy / affinity, member re-detection.
	r.Get("/groups/{name}/regions", getSmartRegions(ctx))
	r.Put("/groups/{name}/regions", updateSmartRegions(ctx))
	r.Delete("/groups/{name}/regions/lock", unlockSmartRegion(ctx))
	r.Post("/groups/{name}/regions/detect", redetectSmartRegions(ctx))
	r.Put("/groups/{name}/balance", updateSmartBalance(ctx))
	return r
}

func getAllSmartWeights(ctx context.Context) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		groups, err := smartapi.Groups(ctx)
		if err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		result, errorsMap := smartapi.Weights(groups, r.URL.Query().Get("refresh") == "true")
		if len(result) == 0 && len(errorsMap) == 0 {
			render.JSON(w, r, render.M{"weights": map[string]any{}, "message": "No Smart groups or no weight data available"})
			return
		}
		render.JSON(w, r, render.M{"weights": result, "errors": errorsMap})
	}
}

// smartGroupInfo is the per-group summary of GET /smart/groups.
func smartGroupInfo(sg *group.Smart) map[string]any {
	summary := smartapi.Summarize(sg)
	info := map[string]any{
		"name":           summary.Tag,
		"type":           summary.Type,
		"testUrl":        summary.TestURL,
		"expectedStatus": summary.ExpectedStatus,
		"preferASN":      summary.PreferASN,
		"useLightGBM":    summary.UseLightGBM,
		"collectData":    summary.CollectData,
		"sampleRate":     summary.SampleRate,
		"tolerance":      summary.Tolerance,
		"fixed":          summary.Fixed,
		"now":            summary.Now,
		"members":        summary.MemberCount,
	}
	if len(summary.PolicyPriority) > 0 {
		priority := make([]map[string]any, len(summary.PolicyPriority))
		for i, rule := range summary.PolicyPriority {
			priority[i] = map[string]any{"pattern": rule.Pattern, "factor": rule.Factor}
		}
		info["policyPriority"] = priority
	}
	if summary.LGBMModelAge != "" {
		info["lgbmModelAge"] = summary.LGBMModelAge
	}
	if sg.IsLoadBalance() {
		info["loadBalance"] = sg.BalanceInfo()
	}
	return info
}

func listSmartGroups(ctx context.Context) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		groups, err := smartapi.Groups(ctx)
		if err != nil {
			render.Status(r, http.StatusInternalServerError)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		infos := make([]map[string]any, 0, len(groups))
		for _, sg := range groups {
			infos = append(infos, smartGroupInfo(sg))
		}
		render.JSON(w, r, render.M{"groups": infos})
	}
}

// smartGroupDiag returns the group's selection state: members with their
// health, stability and blocks; the per-network selection reasons, switch
// counts, serving pool and exploration budgets; host block records; and
// the evidence cache sizes.
func smartGroupDiag(ctx context.Context) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		sg, err := smartapi.Resolve(ctx, chi.URLParam(r, "name"))
		if err != nil {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		diag := sg.Diagnostic()
		if ranking, err := sg.WeightRanking(false); err == nil {
			diag["ranking"] = ranking
		}
		render.JSON(w, r, diag)
	}
}

func recomputeSmartWeights(ctx context.Context) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		sg, err := smartapi.Resolve(ctx, chi.URLParam(r, "name"))
		if err != nil {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		ranking, _ := sg.WeightRanking(true)
		render.JSON(w, r, render.M{"group": sg.Tag(), "ranking": ranking})
	}
}

func clearSmartSelection(ctx context.Context) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		sg, err := smartapi.Resolve(ctx, chi.URLParam(r, "name"))
		if err != nil {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		render.JSON(w, r, sg.ClearSelection())
	}
}

// blockSmartNode blocks a member for the whole group, for ?duration= (Go
// duration, default 30m).
func blockSmartNode(ctx context.Context) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		sg, err := smartapi.Resolve(ctx, chi.URLParam(r, "name"))
		if err != nil {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		node := chi.URLParam(r, "node")
		duration := group.DefaultBlockDuration
		if d := r.URL.Query().Get("duration"); d != "" {
			if parsed, err := time.ParseDuration(d); err == nil && parsed > 0 {
				duration = parsed
			}
		}
		if err = sg.MarkBlocked(node, duration); err != nil {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		render.JSON(w, r, render.M{
			"group":         sg.Tag(),
			"node":          node,
			"blocked_until": time.Now().Add(duration).Format(time.RFC3339),
		})
	}
}

func unblockSmartNode(ctx context.Context) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		sg, err := smartapi.Resolve(ctx, chi.URLParam(r, "name"))
		if err != nil {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		node := chi.URLParam(r, "node")
		if !sg.HasMember(node) {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, newError("node not a member of this group: "+node))
			return
		}
		sg.UnblockNode(node)
		render.NoContent(w, r)
	}
}

// resolveLoadBalanceGroup resolves a smart-loadbalance group or answers the
// request with the error.
func resolveLoadBalanceGroup(ctx context.Context, w http.ResponseWriter, r *http.Request) (*group.Smart, bool) {
	name := chi.URLParam(r, "name")
	sg, err := smartapi.Resolve(ctx, name)
	if err != nil {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, newError(err.Error()))
		return nil, false
	}
	if !sg.IsLoadBalance() {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("not a smart-loadbalance group: "+name))
		return nil, false
	}
	return sg, true
}

// getSmartRegions returns the region table of a smart-loadbalance group:
// every region with its members, their detection source, health, load and
// quality, plus the live mode / lock / strategy settings.
func getSmartRegions(ctx context.Context) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		sg, ok := resolveLoadBalanceGroup(ctx, w, r)
		if !ok {
			return
		}
		render.JSON(w, r, sg.RegionTable())
	}
}

// updateSmartRegions changes the region lock and / or the region mode.
// Body: {"lock": "JP"} (region code, built-in name or region outbound tag;
// "" unlocks) and / or {"mode": "destination"} ("" restores the configured
// mode). Fields left out are not touched.
func updateSmartRegions(ctx context.Context) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		sg, ok := resolveLoadBalanceGroup(ctx, w, r)
		if !ok {
			return
		}
		var body struct {
			Lock *string `json:"lock"`
			Mode *string `json:"mode"`
		}
		if err := render.DecodeJSON(r.Body, &body); err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError("invalid JSON body: "+err.Error()))
			return
		}
		if body.Lock == nil && body.Mode == nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError(`body must set "lock" and / or "mode"`))
			return
		}
		// Validate both inputs before mutating either field.
		if body.Mode != nil {
			if _, err := sg.ValidateRegionMode(*body.Mode); err != nil {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, newError(err.Error()))
				return
			}
		}
		if body.Lock != nil {
			if _, err := sg.ValidateRegionLock(*body.Lock); err != nil {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, newError(err.Error()))
				return
			}
		}
		response := render.M{"group": sg.Tag()}
		if body.Mode != nil {
			mode, err := sg.SetRegionMode(*body.Mode)
			if err != nil {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, newError(err.Error()))
				return
			}
			response["mode"] = mode
		}
		if body.Lock != nil {
			lock, err := sg.SetRegionLock(*body.Lock)
			if err != nil {
				render.Status(r, http.StatusBadRequest)
				render.JSON(w, r, newError(err.Error()))
				return
			}
			response["lock"] = lock
		}
		response["now"] = sg.Now()
		render.JSON(w, r, response)
	}
}

func unlockSmartRegion(ctx context.Context) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		sg, ok := resolveLoadBalanceGroup(ctx, w, r)
		if !ok {
			return
		}
		previous := sg.RegionLock()
		if _, err := sg.SetRegionLock(""); err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		render.JSON(w, r, render.M{"group": sg.Tag(), "previous": previous, "now": sg.Now()})
	}
}

// redetectSmartRegions re-sorts the members into regions and starts exit
// probes; ?force=true probes every member, not just those that need it.
func redetectSmartRegions(ctx context.Context) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		sg, ok := resolveLoadBalanceGroup(ctx, w, r)
		if !ok {
			return
		}
		force := r.URL.Query().Get("force") == "true" || r.URL.Query().Get("force") == "1"
		started := sg.RedetectRegions(force)
		render.JSON(w, r, render.M{"group": sg.Tag(), "exitProbes": started})
	}
}

// updateSmartBalance switches the balance strategy and / or affinity.
// Body: {"strategy": "least-connections", "affinity": "site"}; "default"
// restores the configured value, fields left out are not touched.
func updateSmartBalance(ctx context.Context) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		sg, ok := resolveLoadBalanceGroup(ctx, w, r)
		if !ok {
			return
		}
		var body struct {
			Strategy string `json:"strategy"`
			Affinity string `json:"affinity"`
		}
		if err := render.DecodeJSON(r.Body, &body); err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError("invalid JSON body: "+err.Error()))
			return
		}
		strategy, affinity, err := sg.SetBalance(body.Strategy, body.Affinity)
		if err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError(err.Error()))
			return
		}
		render.JSON(w, r, render.M{"group": sg.Tag(), "strategy": strategy, "affinity": affinity})
	}
}
