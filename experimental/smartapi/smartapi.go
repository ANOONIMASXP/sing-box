package smartapi

import (
	"context"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/smart"
	"github.com/sagernet/sing-box/protocol/group"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
)

// Groups returns every Smart group (smart and smart-loadbalance) from the
// outbound manager. smart-region outbounds are deliberately excluded; they
// are controlled through their parent Smart group.
func GroupsFromManager(manager adapter.OutboundManager) ([]*group.Smart, error) {
	if manager == nil {
		return nil, E.New("outbound manager unavailable")
	}
	var groups []*group.Smart
	for _, outbound := range manager.Outbounds() {
		if smartGroup, loaded := outbound.(*group.Smart); loaded {
			groups = append(groups, smartGroup)
		}
	}
	return groups, nil
}

// Groups is the context-based variant used by the Clash API.
func Groups(ctx context.Context) ([]*group.Smart, error) {
	return GroupsFromManager(service.FromContext[adapter.OutboundManager](ctx))
}

// ResolveFromManager resolves one Smart group by tag.
func ResolveFromManager(manager adapter.OutboundManager, tag string) (*group.Smart, error) {
	if manager == nil {
		return nil, E.New("outbound manager unavailable")
	}
	outbound, loaded := manager.Outbound(tag)
	if !loaded {
		return nil, E.New("group not found: ", tag)
	}
	smartGroup, loaded := outbound.(*group.Smart)
	if !loaded {
		return nil, E.New("not a Smart group: ", tag)
	}
	return smartGroup, nil
}

// Resolve is the context-based variant used by the Clash API.
func Resolve(ctx context.Context, tag string) (*group.Smart, error) {
	return ResolveFromManager(service.FromContext[adapter.OutboundManager](ctx), tag)
}

// Weights ranks every group's members concurrently. Groups without ranking
// data are omitted; per-group errors are returned in the second map while
// other groups still produce results.
func Weights(groups []*group.Smart, refresh bool) (map[string][]smart.NodeRank, map[string]string) {
	result := make(map[string][]smart.NodeRank, len(groups))
	errorsMap := make(map[string]string)
	var (
		access sync.Mutex
		wg     sync.WaitGroup
		sem    = make(chan struct{}, 5)
	)
	for _, smartGroup := range groups {
		wg.Add(1)
		sem <- struct{}{}
		go func(smartGroup *group.Smart) {
			defer wg.Done()
			defer func() { <-sem }()
			weights, err := smartGroup.WeightRanking(refresh)
			access.Lock()
			defer access.Unlock()
			if err != nil {
				errorsMap[smartGroup.Tag()] = err.Error()
				return
			}
			result[smartGroup.Tag()] = weights
		}(smartGroup)
	}
	wg.Wait()
	return result, errorsMap
}

// PriorityRule is the typed form of option.SmartPolicyRule for API clients.
type PriorityRule struct {
	Pattern string
	Factor  float64
}

// Summary is the shared Smart group summary used by Clash and the native API.
type Summary struct {
	Tag            string
	Type           string
	TestURL        string
	ExpectedStatus string
	PreferASN      bool
	UseLightGBM    bool
	CollectData    bool
	SampleRate     float64
	Tolerance      uint16
	Fixed          string
	Now            string
	MemberCount    int
	LoadBalance    bool
	PolicyPriority []PriorityRule
	LGBMModelAge   string
}

// Summarize extracts the stable group summary fields in one place.
func Summarize(smartGroup *group.Smart) Summary {
	summary := Summary{
		Tag:            smartGroup.Tag(),
		Type:           smartGroup.Type(),
		TestURL:        smartGroup.TestURL(),
		ExpectedStatus: smartGroup.ExpectedStatus(),
		PreferASN:      smartGroup.PreferASN(),
		UseLightGBM:    smartGroup.UseLightGBM(),
		CollectData:    smartGroup.CollectData(),
		SampleRate:     smartGroup.SampleRate(),
		Tolerance:      smartGroup.Tolerance(),
		Fixed:          smartGroup.FixedSelection(),
		Now:            smartGroup.Now(),
		MemberCount:    len(smartGroup.MemberTags()),
		LoadBalance:    smartGroup.IsLoadBalance(),
	}
	if rules := smartGroup.PolicyPriority(); len(rules) > 0 {
		summary.PolicyPriority = make([]PriorityRule, len(rules))
		for index, rule := range rules {
			summary.PolicyPriority[index] = PriorityRule{Pattern: rule.Pattern, Factor: rule.Factor}
		}
	}
	if age := smartGroup.LGBMModelAge(); age > 0 {
		summary.LGBMModelAge = age.Round(time.Second).String()
	}
	return summary
}
