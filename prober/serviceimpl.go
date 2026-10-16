// Copyright 2019 The Cloudprober Authors.
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
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	configpb "github.com/cloudprober/cloudprober/config/proto"
	"github.com/cloudprober/cloudprober/logger"
	"github.com/cloudprober/cloudprober/logger/logstore"
	"github.com/cloudprober/cloudprober/metrics/singlerun"
	pb "github.com/cloudprober/cloudprober/prober/proto"
	probes_configpb "github.com/cloudprober/cloudprober/probes/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// saveProbesConfigToFile saves the current config to the given file path.
// This function is called whenever we change the config in response to a gRPC
// request.
func (pr *Prober) saveProbesConfigToFile(filePath string) error {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	var keys []string
	for k := range pr.Probes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	cfg := &configpb.ProberConfig{}
	for _, k := range keys {
		cfg.Probe = append(cfg.Probe, pr.Probes[k].ProbeDef)
	}

	textCfg := prototext.MarshalOptions{
		Indent: "  ",
	}.Format(cfg)

	if textCfg == "" && len(pr.Probes) != 0 {
		err := fmt.Errorf("text marshaling of probes config returned an empty string. Config: %v", cfg)
		pr.l.Warning(err.Error())
		return err
	}

	if err := os.WriteFile(filePath, []byte(textCfg), 0644); err != nil {
		pr.l.Errorf("Error saving config to disk: %v", err)
		return err
	}

	return nil
}

// AddProbe adds the given probe to cloudprober.
func (pr *Prober) AddProbe(ctx context.Context, req *pb.AddProbeRequest) (*pb.AddProbeResponse, error) {
	pr.operations.Lock()
	defer pr.operations.Unlock()
	pr.mu.RLock()
	ready := !pr.stopped && pr.startCtx != nil && pr.startCtx.Err() == nil
	pr.mu.RUnlock()
	if !ready {
		return nil, status.Error(codes.FailedPrecondition, "prober not running")
	}

	pr.l.Info("AddProbe called")

	p := req.GetProbeConfig()
	if p == nil {
		return &pb.AddProbeResponse{}, status.Errorf(codes.InvalidArgument, "probe config cannot be nil")
	}

	if err := pr.addProbe(p); err != nil {
		return &pb.AddProbeResponse{}, err
	}
	pr.warnLatencyTypeClash(p.GetName())

	// Start probe using the prober's start context.
	pr.startProbe(p.GetName())

	if *probesConfigSavePath != "" {
		pr.saveProbesConfigToFile(*probesConfigSavePath)
	}

	return &pb.AddProbeResponse{}, nil
}

func (pr *Prober) RunProbe(ctx context.Context, req *pb.RunProbeRequest) (*pb.RunProbeResponse, error) {
	results, err := pr.Run(ctx, req.GetProbeName())
	if err != nil {
		return nil, err
	}

	return &pb.RunProbeResponse{
		Results: singlerun.ProbeRunResultsToProto(results),
	}, nil
}

// removeProbe removes the probe with the given name from the prober's internal
// database and cancels the probe's context.
func (pr *Prober) removeProbe(name string) error {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if pr.stopped {
		return status.Error(codes.FailedPrecondition, "prober stopped")
	}
	p := pr.Probes[name]
	if p == nil {
		return status.Errorf(codes.NotFound, "probe %s not found", name)
	}
	if cancel := pr.probeCancelFunc[name]; cancel != nil {
		cancel()
	}
	delete(pr.probeCancelFunc, name)
	delete(pr.Probes, name)
	closeProbe(p)
	return nil
}

// RemoveProbe gRPC method cancels the given probe and removes its from the
// prober's internal database.
func (pr *Prober) RemoveProbe(ctx context.Context, req *pb.RemoveProbeRequest) (*pb.RemoveProbeResponse, error) {
	pr.operations.Lock()
	defer pr.operations.Unlock()
	pr.l.Infof("RemoveProbe called with: %s", req.GetProbeName())

	name := req.GetProbeName()
	if name == "" {
		return &pb.RemoveProbeResponse{}, status.Errorf(codes.InvalidArgument, "probe name cannot be empty")
	}

	if err := pr.removeProbe(name); err != nil {
		return &pb.RemoveProbeResponse{}, err
	}

	if *probesConfigSavePath != "" {
		pr.saveProbesConfigToFile(*probesConfigSavePath)
	}

	return &pb.RemoveProbeResponse{}, nil
}

// GetProbeStatus returns the ongoing probe status data.
func (pr *Prober) GetProbeStatus(ctx context.Context, req *pb.GetProbeStatusRequest) (*pb.GetProbeStatusResponse, error) {
	if err := pr.checkRunning(); err != nil {
		return nil, err
	}
	// A retiring surfacer no longer services requests. Cancel in-flight queries
	// with its runtime so the old gRPC transport can finish draining.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if pr.initCtx != nil {
		stop := context.AfterFunc(pr.initCtx, cancel)
		defer stop()
	}
	if pr.probeStatusSurfacer == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "probestatus surfacer is not available")
	}

	if req.GetTimeWindowMinutes() <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "time_window_minutes must be > 0")
	}

	timeWindow := time.Duration(req.GetTimeWindowMinutes()) * time.Minute
	var endTime time.Time
	if req.EndTimeSec != nil {
		endTime = time.Unix(req.GetEndTimeSec(), 0)
	}

	results, err := pr.probeStatusSurfacer.QueryStatus(ctx, req.GetProbeName(), endTime, timeWindow)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.Internal, "error querying probe status: %v", err)
	}

	resp := &pb.GetProbeStatusResponse{}
	for _, psd := range results {
		probeStatus := &pb.ProbeStatus{Name: proto.String(psd.Name)}
		for _, tsd := range psd.TargetStatuses {
			targetStatus := &pb.TargetStatus{
				TargetName: proto.String(tsd.TargetName),
				Total:      proto.Int64(tsd.Total),
				Success:    proto.Int64(tsd.Success),
			}
			probeStatus.TargetStatus = append(probeStatus.TargetStatus, targetStatus)
		}

		if req.GetIncludeLogs() {
			if ls := logger.DefaultLogStore(); ls != nil {
				logEntries := ls.Query(logstore.QueryOpts{
					Source: psd.Name,
					Limit:  int(req.GetLogLimit()),
				})
				for _, le := range logEntries {
					probeStatus.Logs = append(probeStatus.Logs, &pb.LogEntry{
						TimestampSec:  proto.Int64(le.Timestamp.Unix()),
						TimestampNsec: proto.Int32(int32(le.Timestamp.Nanosecond())),
						Level:         proto.String(le.Level.String()),
						Message:       proto.String(le.Message),
						Attributes:    le.Attrs,
					})
				}
			}
		}

		resp.ProbeStatus = append(resp.ProbeStatus, probeStatus)
	}
	return resp, nil
}

// ListProbes gRPC method returns the list of probes from the in-memory database.
func (pr *Prober) ListProbes(ctx context.Context, req *pb.ListProbesRequest) (*pb.ListProbesResponse, error) {
	if err := pr.checkRunning(); err != nil {
		return nil, err
	}
	pr.l.Info("ListProbes called")
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	resp := &pb.ListProbesResponse{}

	for name, p := range pr.Probes {
		resp.Probe = append(resp.Probe, &pb.Probe{
			Name:   proto.String(name),
			Config: proto.Clone(p.ProbeDef).(*probes_configpb.ProbeDef),
		})
	}

	return resp, nil
}

func (pr *Prober) SaveProbesConfig(ctx context.Context, req *pb.SaveProbesConfigRequest) (*pb.SaveProbesConfigResponse, error) {
	if err := pr.checkRunning(); err != nil {
		return nil, err
	}
	filePath := req.GetFilePath()
	if filePath == "" {
		filePath = *probesConfigSavePath
	}
	if filePath == "" {
		return nil, errors.New("file_path not provided and --config_save_path flag is also not set")
	}

	if err := pr.saveProbesConfigToFile(filePath); err != nil {
		return nil, err
	}

	return &pb.SaveProbesConfigResponse{
		FilePath: &filePath,
	}, nil
}

// checkRunning rejects requests handled by a retired runtime.
func (pr *Prober) checkRunning() error {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	if pr.stopped {
		return status.Error(codes.FailedPrecondition, "prober stopped")
	}
	return nil
}

// ReloadConfig delegates to the owner, which can replace all runtime services.
// No prober lock is held while calling it: the callback stops this instance.
func (pr *Prober) ReloadConfig(ctx context.Context, req *pb.ReloadConfigRequest) (*pb.ReloadConfigResponse, error) {
	pr.mu.RLock()
	f, stopped := pr.reloadConfig, pr.stopped
	pr.mu.RUnlock()
	if stopped {
		return nil, status.Error(codes.FailedPrecondition, "prober stopped")
	}
	if f == nil {
		return nil, status.Error(codes.FailedPrecondition, "config reload is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := f(ctx); err != nil {
		return nil, err
	}
	return &pb.ReloadConfigResponse{}, nil
}
