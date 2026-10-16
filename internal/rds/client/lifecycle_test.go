// Copyright 2026 The Cloudprober Authors.
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

package client

import (
	"context"
	"testing"
	"time"

	configpb "github.com/cloudprober/cloudprober/internal/rds/client/proto"
	pb "github.com/cloudprober/cloudprober/internal/rds/proto"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/proto"
)

func TestContextCancelsInFlightRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	cl, err := NewWithContext(ctx, &configpb.ClientConf{ReEvalSec: proto.Int32(0)}, func(ctx context.Context, _ *pb.ListResourcesRequest) (*pb.ListResourcesResponse, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	done := make(chan struct{})
	go func() { defer close(done); cl.ListEndpoints() }()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("in-flight refresh did not observe runtime cancellation")
	}
	// A stopped client must not issue further requests.
	cl.ListEndpoints()
}

func TestClientCloseStopsRefreshAndConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cl, err := NewWithContext(ctx, &configpb.ClientConf{
		ReEvalSec:     proto.Int32(0),
		ServerOptions: &configpb.ClientConf_ServerOptions{ServerAddress: proto.String("passthrough:///127.0.0.1:1")},
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.Close(); err != nil {
		t.Fatal(err)
	}
	if cl.conn.GetState() != connectivity.Shutdown {
		t.Fatalf("connection state = %v, want shutdown", cl.conn.GetState())
	}
	if err := cl.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestContextStopsPeriodicRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cl, err := NewWithContext(ctx, &configpb.ClientConf{ReEvalSec: proto.Int32(3600)}, func(context.Context, *pb.ListResourcesRequest) (*pb.ListResourcesResponse, error) {
		return &pb.ListResourcesResponse{}, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-cl.done:
	case <-time.After(time.Second):
		t.Fatal("client refresh worker did not stop")
	}
}
