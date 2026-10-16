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

package udp

import (
	"context"
	"net"
	"testing"

	configpb "github.com/cloudprober/cloudprober/internal/servers/udp/proto"
	"github.com/cloudprober/cloudprober/logger"
	"google.golang.org/protobuf/proto"
)

func TestCloseReleasesListenerBeforeStart(t *testing.T) {
	s, err := New(context.Background(), &configpb.ServerConf{Port: proto.Int32(0)}, &logger.Logger{})
	if err != nil {
		t.Fatal(err)
	}
	addr := s.conn.LocalAddr().(*net.UDPAddr)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("socket was not released: %v", err)
	}
	conn.Close()
}
