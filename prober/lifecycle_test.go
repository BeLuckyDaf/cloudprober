// Copyright 2017-2026 The Cloudprober Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prober

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	configpb "github.com/cloudprober/cloudprober/config/proto"
	"github.com/cloudprober/cloudprober/metrics"
	"github.com/cloudprober/cloudprober/metrics/singlerun"
	pb "github.com/cloudprober/cloudprober/prober/proto"
	"github.com/cloudprober/cloudprober/probes"
	"github.com/cloudprober/cloudprober/probes/options"
	probepb "github.com/cloudprober/cloudprober/probes/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type lifecycleProbe struct {
	started chan struct{}
	exited  chan struct{}
	closed  atomic.Bool
}

func (p *lifecycleProbe) Init(string, *options.Options) error { return nil }
func (p *lifecycleProbe) Start(ctx context.Context, _ chan *metrics.EventMetrics) {
	close(p.started)
	<-ctx.Done()
	close(p.exited)
}
func (p *lifecycleProbe) Close() error { p.closed.Store(true); return nil }
func (p *lifecycleProbe) RunOnce(ctx context.Context) []*singlerun.ProbeRunResult {
	close(p.started)
	<-ctx.Done()
	close(p.exited)
	return nil
}
func newLifecycleProber(p *lifecycleProbe, delay uint32) *Prober {
	cfg := &probepb.ProbeDef{Name: proto.String("lifecycle"), StartupDelayMsec: proto.Uint32(delay)}
	return &Prober{
		c:               &configpb.ProberConfig{DisableJitter: proto.Bool(true)},
		Probes:          map[string]*probes.ProbeInfo{"lifecycle": {Probe: p, ProbeDef: cfg, Name: "lifecycle", Options: &options.Options{Interval: time.Hour}}},
		probeCancelFunc: make(map[string]context.CancelFunc),
	}
}
func waitLifecycle(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("lifecycle operation did not complete")
	}
}
func TestStopCancelsPendingStartupAndClosesResources(t *testing.T) {
	p := &lifecycleProbe{started: make(chan struct{}), exited: make(chan struct{})}
	pr := newLifecycleProber(p, 3600000)
	pr.Start(context.Background())
	done := make(chan struct{})
	go func() { pr.Stop(); pr.Stop(); close(done) }()
	waitLifecycle(t, done)
	if !p.closed.Load() {
		t.Error("initialized probe resource was not closed")
	}
	select {
	case <-p.started:
		t.Error("delayed probe started during shutdown")
	default:
	}
	if _, err := pr.AddProbe(context.Background(), &pb.AddProbeRequest{ProbeConfig: pdefForLifecycle()}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("AddProbe after Stop: %v", err)
	}
	if _, err := pr.ListProbes(context.Background(), &pb.ListProbesRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("ListProbes after Stop: %v", err)
	}
}
func pdefForLifecycle() *probepb.ProbeDef { return &probepb.ProbeDef{Name: proto.String("new")} }
func TestStopCancelsActiveRun(t *testing.T) {
	p := &lifecycleProbe{started: make(chan struct{}), exited: make(chan struct{})}
	pr := newLifecycleProber(p, 0)
	pr.startCtx, pr.cancelStart = context.WithCancel(context.Background())
	ran := make(chan struct{})
	go func() { defer close(ran); _, _ = pr.Run(context.Background(), nil) }()
	waitLifecycle(t, p.started)
	stopped := make(chan struct{})
	go func() { pr.Stop(); close(stopped) }()
	waitLifecycle(t, stopped)
	waitLifecycle(t, ran)
	waitLifecycle(t, p.exited)
}
func TestReloadConfigCallbackCanStopProber(t *testing.T) {
	pr := &Prober{}
	called := false
	pr.SetReloadConfigFunc(func(context.Context) error { pr.Stop(); called = true; return nil })
	if _, err := pr.ReloadConfig(context.Background(), &pb.ReloadConfigRequest{}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("reload callback was not called")
	}
	if _, err := pr.ReloadConfig(context.Background(), &pb.ReloadConfigRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("ReloadConfig after Stop: %v", err)
	}
}
