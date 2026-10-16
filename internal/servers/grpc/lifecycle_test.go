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

package grpc

import (
	"context"
	"net"
	"testing"
	"time"

	configpb "github.com/cloudprober/cloudprober/internal/servers/grpc/proto"
	"github.com/cloudprober/cloudprober/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"
)

func TestCloseReleasesDedicatedListenerBeforeStart(t *testing.T) {
	s, err := New(context.Background(), &configpb.ServerConf{Port: proto.Int32(0), UseDedicatedServer: proto.Bool(true)}, &logger.Logger{})
	if err != nil {
		t.Fatal(err)
	}
	addr := s.ln.Addr().String()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listener was not released: %v", err)
	}
	ln.Close()
}

func TestClosePreservesSharedServer(t *testing.T) {
	shared := grpc.NewServer()
	s := &Server{grpcSrv: shared}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	healthpb.RegisterHealthServer(shared, health.NewServer())
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go shared.Serve(ln)
	defer shared.Stop()
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("shared gRPC server was stopped by Close: %v", err)
	}
}
