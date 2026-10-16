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

package servers

import (
	"context"
	"net"
	"testing"

	httppb "github.com/cloudprober/cloudprober/internal/servers/http/proto"
	serverpb "github.com/cloudprober/cloudprober/internal/servers/proto"
	udppb "github.com/cloudprober/cloudprober/internal/servers/udp/proto"
	"google.golang.org/protobuf/proto"
)

func TestInitFailureReleasesEarlierListeners(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	port := int32(listener.Addr().(*net.TCPAddr).Port)
	addr := listener.Addr().String()
	listener.Close()
	_, err = Init(context.Background(), []*serverpb.ServerDef{
		{Type: serverpb.ServerDef_HTTP.Enum(), Server: &serverpb.ServerDef_HttpServer{HttpServer: &httppb.ServerConf{Port: proto.Int32(port)}}},
		{Type: serverpb.ServerDef_UDP.Enum(), Server: &serverpb.ServerDef_UdpServer{UdpServer: &udppb.ServerConf{MaxPayloadSize: proto.Int32(-1)}}},
	})
	if err == nil {
		t.Fatal("Init succeeded with an invalid UDP payload size")
	}
	listener, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Init failure left the HTTP listener open: %v", err)
	}
	listener.Close()
}
