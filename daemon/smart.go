package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/smart"
	"github.com/sagernet/sing-box/experimental/smartapi"
	"github.com/sagernet/sing-box/protocol/group"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

type smartSession struct {
	manager adapter.OutboundManager
	unlock  func()
}

// openSmartSession holds the service read lock for the whole RPC so a reload
// cannot clear s.instance and close the old instance while Smart work runs.
func (s *StartedService) openSmartSession() (*smartSession, error) {
	s.serviceAccess.RLock()
	if s.instance == nil || s.serviceStatus == nil || s.serviceStatus.Status != ServiceStatus_STARTED {
		s.serviceAccess.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, "service is not started")
	}
	if s.instance.outboundManager == nil {
		s.serviceAccess.RUnlock()
		return nil, status.Error(codes.Unavailable, "outbound manager unavailable")
	}
	return &smartSession{
		manager: s.instance.outboundManager,
		unlock:  s.serviceAccess.RUnlock,
	}, nil
}

func smartResolveError(err error) error {
	switch {
	case strings.Contains(err.Error(), "group not found"):
		return status.Error(codes.NotFound, err.Error())
	case strings.Contains(err.Error(), "not a Smart group"):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func smartLoadBalanceGroup(manager adapter.OutboundManager, tag string) (*group.Smart, error) {
	smartGroup, err := smartapi.ResolveFromManager(manager, tag)
	if err != nil {
		return nil, smartResolveError(err)
	}
	if !smartGroup.IsLoadBalance() {
		return nil, status.Error(codes.FailedPrecondition, "not a smart-loadbalance group: "+tag)
	}
	return smartGroup, nil
}

func smartGroupSummaryProto(smartGroup *group.Smart) *SmartGroupSummary {
	summary := smartapi.Summarize(smartGroup)
	result := &SmartGroupSummary{
		Tag:            summary.Tag,
		Type:           summary.Type,
		TestUrl:        summary.TestURL,
		ExpectedStatus: summary.ExpectedStatus,
		PreferAsn:      summary.PreferASN,
		UseLightGBM:    summary.UseLightGBM,
		CollectData:    summary.CollectData,
		SampleRate:     summary.SampleRate,
		Tolerance:      uint32(summary.Tolerance),
		Fixed:          summary.Fixed,
		Now:            summary.Now,
		MemberCount:    uint32(summary.MemberCount),
		LoadBalance:    summary.LoadBalance,
		LgbmModelAge:   summary.LGBMModelAge,
	}
	if len(summary.PolicyPriority) > 0 {
		result.PolicyPriority = make([]*SmartPriorityRule, len(summary.PolicyPriority))
		for index, rule := range summary.PolicyPriority {
			result.PolicyPriority[index] = &SmartPriorityRule{
				Pattern: rule.Pattern,
				Factor:  rule.Factor,
			}
		}
	}
	return result
}

func smartNodeRanksProto(ranking []smart.NodeRank) []*SmartNodeRank {
	if len(ranking) == 0 {
		return nil
	}
	result := make([]*SmartNodeRank, len(ranking))
	for index, rank := range ranking {
		result[index] = &SmartNodeRank{
			Name:        rank.Name,
			Rank:        rank.Rank,
			Weight:      rank.Weight,
			Alive:       rank.Alive,
			Pool:        rank.Pool,
			Utility:     rank.Utility,
			Reliability: rank.Reliability,
			Observed:    rank.Observed,
			Quality:     rank.Quality,
			Completed:   rank.Completed,
			Qualified:   rank.Qualified,
			FailStreak:  rank.FailStreak,
			BackedOff:   rank.BackedOff,
			ProbeMs:     rank.ProbeMS,
			ResponseMs:  rank.ResponseMS,
			SetupMs:     rank.SetupMS,
			UploadBps:   rank.UploadBps,
			DownloadBps: rank.DownloadBps,
			Loss:        rank.Loss,
			Success:     rank.Success,
			Failure:     rank.Failure,
			Targets:     int32(rank.Targets),
		}
	}
	return result
}

func smartMembersProto(members []group.MemberInfo) []*SmartMember {
	if len(members) == 0 {
		return nil
	}
	result := make([]*SmartMember, len(members))
	for index, member := range members {
		result[index] = &SmartMember{
			Name:           member.Name,
			Alive:          member.Alive,
			Delay:          uint32(member.Delay),
			StabilityGrade: int32(member.Grade),
			Blocked:        member.Blocked,
			BlockedUntil:   member.BlockedUntil,
			Load:           member.Load,
			Priority:       member.Priority,
			ExitRegion:     member.ExitRegion,
			ExitAsn:        member.ExitASN,
		}
	}
	return result
}

func smartRegionTableFromMap(value map[string]any) (*SmartRegionTable, error) {
	if value == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var table SmartRegionTable
	err = protojson.Unmarshal(encoded, &table)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, encoded)
	}
	return &table, nil
}

func smartBalanceInfoFromMap(value map[string]any) (*SmartBalanceInfo, error) {
	if value == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var info SmartBalanceInfo
	err = protojson.Unmarshal(encoded, &info)
	if err != nil {
		return nil, err
	}
	return &info, nil
}

func resolveSmartGroup(session *smartSession, tag string) (*group.Smart, error) {
	smartGroup, err := smartapi.ResolveFromManager(session.manager, tag)
	if err != nil {
		return nil, smartResolveError(err)
	}
	return smartGroup, nil
}

func (s *StartedService) GetSmartGroups(ctx context.Context, empty *emptypb.Empty) (*SmartGroups, error) {
	session, err := s.openSmartSession()
	if err != nil {
		return nil, err
	}
	defer session.unlock()
	manager := session.manager
	groups, err := smartapi.GroupsFromManager(manager)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	result := &SmartGroups{Groups: make([]*SmartGroupSummary, 0, len(groups))}
	for _, smartGroup := range groups {
		result.Groups = append(result.Groups, smartGroupSummaryProto(smartGroup))
	}
	return result, nil
}

func (s *StartedService) GetSmartGroup(ctx context.Context, request *SmartGroupRequest) (*SmartGroup, error) {
	session, err := s.openSmartSession()
	if err != nil {
		return nil, err
	}
	defer session.unlock()
	smartGroup, err := resolveSmartGroup(session, request.Tag)
	if err != nil {
		return nil, err
	}
	result := &SmartGroup{
		Summary: smartGroupSummaryProto(smartGroup),
		Members: smartMembersProto(smartGroup.MemberInfo()),
	}
	if request.IncludeDiagnosticJson {
		diagnostic := smartGroup.Diagnostic()
		diagnosticJSON, err := json.Marshal(diagnostic)
		if err != nil {
			return nil, status.Error(codes.Internal, "marshal Smart diagnostic: "+err.Error())
		}
		result.DiagnosticJson = string(diagnosticJSON)
	}
	if ranking, rankingErr := smartGroup.WeightRanking(false); rankingErr == nil {
		result.Ranking = smartNodeRanksProto(ranking)
	}
	result.RegionTable, err = smartRegionTableFromMap(smartGroup.RegionTable())
	if err != nil {
		return nil, status.Error(codes.Internal, "convert Smart regions: "+err.Error())
	}
	if result.RegionTable != nil {
		// RegionTable already carries the region list; keep only the compact
		// scalar block in balanceInfo to avoid sending/converting it twice.
		result.BalanceInfo = &SmartBalanceInfo{
			Strategy: result.RegionTable.Strategy,
			Affinity: result.RegionTable.Affinity,
			Mode:     result.RegionTable.Mode,
			Lock:     result.RegionTable.Lock,
			Fallback: result.RegionTable.Fallback,
		}
	} else {
		result.BalanceInfo, err = smartBalanceInfoFromMap(smartGroup.BalanceInfo())
		if err != nil {
			return nil, status.Error(codes.Internal, "convert Smart balance: "+err.Error())
		}
	}
	return result, nil
}

func (s *StartedService) GetSmartWeights(ctx context.Context, empty *emptypb.Empty) (*SmartWeights, error) {
	session, err := s.openSmartSession()
	if err != nil {
		return nil, err
	}
	defer session.unlock()
	manager := session.manager
	groups, err := smartapi.GroupsFromManager(manager)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	weights, weightErrors := smartapi.Weights(groups, false)
	result := &SmartWeights{
		Weights: make(map[string]*SmartRanking, len(weights)),
		Errors:  make(map[string]string, len(weightErrors)),
	}
	for tag, ranking := range weights {
		result.Weights[tag] = &SmartRanking{Items: smartNodeRanksProto(ranking)}
	}
	for tag, message := range weightErrors {
		result.Errors[tag] = message
	}
	return result, nil
}

func (s *StartedService) RecomputeSmartWeights(ctx context.Context, request *SmartGroupRequest) (*SmartRanking, error) {
	session, err := s.openSmartSession()
	if err != nil {
		return nil, err
	}
	defer session.unlock()
	smartGroup, err := resolveSmartGroup(session, request.Tag)
	if err != nil {
		return nil, err
	}
	ranking, err := smartGroup.WeightRanking(true)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &SmartRanking{Items: smartNodeRanksProto(ranking)}, nil
}

func (s *StartedService) BlockSmartNode(ctx context.Context, request *SmartBlockNodeRequest) (*emptypb.Empty, error) {
	session, err := s.openSmartSession()
	if err != nil {
		return nil, err
	}
	defer session.unlock()
	smartGroup, err := resolveSmartGroup(session, request.Tag)
	if err != nil {
		return nil, err
	}
	duration := time.Duration(request.Duration)
	if duration <= 0 {
		duration = group.DefaultBlockDuration
	}
	if err = smartGroup.MarkBlocked(request.Node, duration); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *StartedService) UnblockSmartNode(ctx context.Context, request *SmartNodeRequest) (*emptypb.Empty, error) {
	session, err := s.openSmartSession()
	if err != nil {
		return nil, err
	}
	defer session.unlock()
	smartGroup, err := resolveSmartGroup(session, request.Tag)
	if err != nil {
		return nil, err
	}
	if !smartGroup.HasMember(request.Node) {
		return nil, status.Error(codes.NotFound, "node not a member of this group: "+request.Node)
	}
	smartGroup.UnblockNode(request.Node)
	return &emptypb.Empty{}, nil
}

func (s *StartedService) ClearSmartSelection(ctx context.Context, request *SmartGroupRequest) (*SmartClearSelectionResult, error) {
	session, err := s.openSmartSession()
	if err != nil {
		return nil, err
	}
	defer session.unlock()
	smartGroup, err := resolveSmartGroup(session, request.Tag)
	if err != nil {
		return nil, err
	}
	result := smartGroup.ClearSelection()
	return &SmartClearSelectionResult{
		Group:       result.Group,
		PreviousPin: result.PreviousPin,
		Now:         result.Now,
		Interrupted: result.Interrupted,
	}, nil
}

func (s *StartedService) GetSmartRegions(ctx context.Context, request *SmartGroupRequest) (*SmartRegionTable, error) {
	session, err := s.openSmartSession()
	if err != nil {
		return nil, err
	}
	defer session.unlock()
	manager := session.manager
	smartGroup, err := smartLoadBalanceGroup(manager, request.Tag)
	if err != nil {
		return nil, err
	}
	table, err := smartRegionTableFromMap(smartGroup.RegionTable())
	if err != nil {
		return nil, status.Error(codes.Internal, "convert Smart regions: "+err.Error())
	}
	return table, nil
}

func (s *StartedService) UpdateSmartRegions(ctx context.Context, request *SmartUpdateRegionsRequest) (*SmartRegionTable, error) {
	session, err := s.openSmartSession()
	if err != nil {
		return nil, err
	}
	defer session.unlock()
	manager := session.manager
	smartGroup, err := smartLoadBalanceGroup(manager, request.Tag)
	if err != nil {
		return nil, err
	}
	if !request.SetLock && !request.SetMode {
		return nil, status.Error(codes.InvalidArgument, "setLock and / or setMode is required")
	}
	// Validate both inputs before mutating either field.
	if request.SetMode {
		if _, err = smartGroup.ValidateRegionMode(request.Mode); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	if request.SetLock {
		if _, err = smartGroup.ValidateRegionLock(request.Lock); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	if request.SetMode {
		if _, err = smartGroup.SetRegionMode(request.Mode); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	if request.SetLock {
		if _, err = smartGroup.SetRegionLock(request.Lock); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	table, err := smartRegionTableFromMap(smartGroup.RegionTable())
	if err != nil {
		return nil, status.Error(codes.Internal, "convert Smart regions: "+err.Error())
	}
	return table, nil
}

func (s *StartedService) UnlockSmartRegion(ctx context.Context, request *SmartGroupRequest) (*SmartRegionTable, error) {
	session, err := s.openSmartSession()
	if err != nil {
		return nil, err
	}
	defer session.unlock()
	manager := session.manager
	smartGroup, err := smartLoadBalanceGroup(manager, request.Tag)
	if err != nil {
		return nil, err
	}
	if _, err = smartGroup.SetRegionLock(""); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	table, err := smartRegionTableFromMap(smartGroup.RegionTable())
	if err != nil {
		return nil, status.Error(codes.Internal, "convert Smart regions: "+err.Error())
	}
	return table, nil
}

func (s *StartedService) RedetectSmartRegions(ctx context.Context, request *SmartRedetectRegionsRequest) (*emptypb.Empty, error) {
	session, err := s.openSmartSession()
	if err != nil {
		return nil, err
	}
	defer session.unlock()
	manager := session.manager
	smartGroup, err := smartLoadBalanceGroup(manager, request.Tag)
	if err != nil {
		return nil, err
	}
	smartGroup.RedetectRegions(request.Force)
	return &emptypb.Empty{}, nil
}

func (s *StartedService) UpdateSmartBalance(ctx context.Context, request *SmartUpdateBalanceRequest) (*SmartBalanceInfo, error) {
	session, err := s.openSmartSession()
	if err != nil {
		return nil, err
	}
	defer session.unlock()
	manager := session.manager
	smartGroup, err := smartLoadBalanceGroup(manager, request.Tag)
	if err != nil {
		return nil, err
	}
	if _, _, err = smartGroup.SetBalance(request.Strategy, request.Affinity); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	info, err := smartBalanceInfoFromMap(smartGroup.BalanceInfo())
	if err != nil {
		return nil, status.Error(codes.Internal, "convert Smart balance: "+err.Error())
	}
	if info == nil {
		info = &SmartBalanceInfo{}
	}
	return info, nil
}
