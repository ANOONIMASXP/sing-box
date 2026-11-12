package daemon

import (
	"context"
	"net"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type testOutboundManager struct {
	outbounds []adapter.Outbound
}

func (m *testOutboundManager) Start(adapter.StartStage, *adapter.Scope) error { return nil }
func (m *testOutboundManager) Close() error                                   { return nil }
func (m *testOutboundManager) Outbounds() []adapter.Outbound                  { return m.outbounds }
func (m *testOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	for _, outbound := range m.outbounds {
		if outbound.Tag() == tag {
			return outbound, true
		}
	}
	return nil, false
}
func (m *testOutboundManager) Default() adapter.Outbound {
	if len(m.outbounds) == 0 {
		return nil
	}
	return m.outbounds[0]
}
func (m *testOutboundManager) Create(context.Context, adapter.Router, log.ContextLogger, string, string, any) error {
	return nil
}
func (m *testOutboundManager) OutboundOption(string) (option.Outbound, bool) {
	return option.Outbound{}, false
}
func (m *testOutboundManager) RegisterOutbound(adapter.Outbound) error { return nil }
func (m *testOutboundManager) UnregisterOutbound(string) error         { return nil }

func newTestSmartService(t *testing.T, outbounds ...adapter.Outbound) *StartedService {
	t.Helper()
	return &StartedService{
		serviceStatus: &ServiceStatus{Status: ServiceStatus_STARTED},
		instance: &Instance{
			outboundManager: &testOutboundManager{outbounds: outbounds},
		},
	}
}

func newTestSmartGroup(t *testing.T, tag string) adapter.Outbound {
	t.Helper()
	logger := log.NewNOPFactory().NewLogger("smart")
	smartGroup, err := group.NewSmart(context.Background(), nil, logger, tag, option.SmartOutboundOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return smartGroup
}

func TestSmartAPIRequiresStartedService(t *testing.T) {
	service := &StartedService{}
	_, err := service.GetSmartGroups(context.Background(), nil)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("GetSmartGroups() error = %v, want FailedPrecondition", err)
	}
}

func TestSmartAPIGroupsAndDetail(t *testing.T) {
	smartGroup := newTestSmartGroup(t, "smart-test")
	service := newTestSmartService(t, smartGroup)

	groups, err := service.GetSmartGroups(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups.Groups) != 1 || groups.Groups[0].Tag != "smart-test" {
		t.Fatalf("GetSmartGroups() = %+v", groups)
	}

	plainDetail, err := service.GetSmartGroup(context.Background(), &SmartGroupRequest{Tag: "smart-test"})
	if err != nil {
		t.Fatal(err)
	}
	if plainDetail.DiagnosticJson != "" {
		t.Fatal("GetSmartGroup() returned diagnostic JSON without the opt-in flag")
	}

	detail, err := service.GetSmartGroup(context.Background(), &SmartGroupRequest{Tag: "smart-test", IncludeDiagnosticJson: true})
	if err != nil {
		t.Fatal(err)
	}
	if detail.Summary == nil || detail.Summary.Tag != "smart-test" {
		t.Fatalf("GetSmartGroup() summary = %+v", detail.Summary)
	}
	if detail.DiagnosticJson == "" {
		t.Fatal("GetSmartGroup() returned an empty diagnostic JSON")
	}

	weights, err := service.GetSmartWeights(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, loaded := weights.Weights["smart-test"]; !loaded {
		if _, errored := weights.Errors["smart-test"]; !errored {
			t.Fatalf("GetSmartWeights() = %+v", weights)
		}
	}

	_, err = service.BlockSmartNode(context.Background(), &SmartBlockNodeRequest{
		Tag:  "smart-test",
		Node: "missing",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("BlockSmartNode() error = %v, want NotFound", err)
	}

	_, err = service.GetSmartRegions(context.Background(), &SmartGroupRequest{Tag: "smart-test"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("GetSmartRegions() error = %v, want FailedPrecondition", err)
	}

	_, err = service.UnblockSmartNode(context.Background(), &SmartNodeRequest{Tag: "smart-test", Node: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("UnblockSmartNode() error = %v, want NotFound", err)
	}
}

func TestSmartAPILoadBalanceRegions(t *testing.T) {
	logger := log.NewNOPFactory().NewLogger("smart-loadbalance")
	loadBalance, err := group.NewSmartLoadBalance(context.Background(), nil, logger, "lb-test", option.SmartLoadBalanceOutboundOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service := newTestSmartService(t, loadBalance)

	table, err := service.GetSmartRegions(context.Background(), &SmartGroupRequest{Tag: "lb-test"})
	if err != nil {
		t.Fatal(err)
	}
	if table.Group != "lb-test" {
		t.Fatalf("GetSmartRegions() group = %q", table.Group)
	}

	info, err := service.UpdateSmartBalance(context.Background(), &SmartUpdateBalanceRequest{
		Tag:      "lb-test",
		Strategy: "round-robin",
		Affinity: "none",
	})
	if err != nil {
		t.Fatal(err)
	}
	if info.Strategy != "round-robin" || info.Affinity != "none" {
		t.Fatalf("UpdateSmartBalance() = %+v", info)
	}

	beforeMode := table.Mode
	_, err = service.UpdateSmartRegions(context.Background(), &SmartUpdateRegionsRequest{
		Tag:     "lb-test",
		Mode:    "off",
		SetMode: true,
		Lock:    "definitely-not-a-region",
		SetLock: true,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateSmartRegions() error = %v, want InvalidArgument", err)
	}
	after, err := service.GetSmartRegions(context.Background(), &SmartGroupRequest{Tag: "lb-test"})
	if err != nil {
		t.Fatal(err)
	}
	if after.Mode != beforeMode {
		t.Fatalf("UpdateSmartRegions() applied mode before lock validation: %q -> %q", beforeMode, after.Mode)
	}
}

func TestSmartAPIGRPCRegistrationAndAuthentication(t *testing.T) {
	service := newTestSmartService(t, newTestSmartGroup(t, "smart-grpc"))
	server := NewServer(service, "test-secret")
	listener := bufconn.Listen(1024 * 1024)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	connection, err := grpc.NewClient(
		"passthrough:///smart-api-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := NewStartedServiceClient(connection)

	_, err = client.GetSmartGroups(context.Background(), &emptypb.Empty{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated GetSmartGroups() error = %v, want Unauthenticated", err)
	}
	authenticatedContext := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer test-secret")
	groups, err := client.GetSmartGroups(authenticatedContext, &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if len(groups.Groups) != 1 || groups.Groups[0].Tag != "smart-grpc" {
		t.Fatalf("GetSmartGroups() = %+v", groups)
	}
}
