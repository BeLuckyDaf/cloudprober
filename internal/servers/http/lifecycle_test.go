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

package http

import (
	"context"
	"net"
	"testing"

	configpb "github.com/cloudprober/cloudprober/internal/servers/http/proto"
	"github.com/cloudprober/cloudprober/logger"
	"google.golang.org/protobuf/proto"
)

func TestCloseReleasesListenerBeforeStart(t *testing.T) {
	s, err := New(context.Background(), &configpb.ServerConf{Port: proto.Int32(0)}, &logger.Logger{})
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

func TestInvalidTLSReleasesListener(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	port := int32(ln.Addr().(*net.TCPAddr).Port)
	addr := ln.Addr().String()
	ln.Close()
	_, err = New(context.Background(), &configpb.ServerConf{Port: proto.Int32(port), Protocol: configpb.ServerConf_HTTPS.Enum()}, &logger.Logger{})
	if err == nil {
		t.Fatal("New accepted HTTPS without a certificate")
	}
	ln, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed TLS initialization leaked listener: %v", err)
	}
	ln.Close()
}

func TestInvalidPatternDataFailsDuringInitialization(t *testing.T) {
	for _, handler := range []*configpb.ServerConf_PatternDataHandler{
		{ResponseSize: proto.Int32(-1)},
		{ResponseSize: proto.Int32(10), Pattern: proto.String("")},
	} {
		s, err := New(context.Background(), &configpb.ServerConf{Port: proto.Int32(0), PatternDataHandler: []*configpb.ServerConf_PatternDataHandler{handler}}, &logger.Logger{})
		if err == nil {
			s.Close()
			t.Fatalf("New accepted invalid pattern data: %v", handler)
		}
	}
}
