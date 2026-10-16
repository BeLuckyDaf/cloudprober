// Copyright 2018-2025 The Cloudprober Authors.
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

package state

import (
	rdsserver "github.com/cloudprober/cloudprober/internal/rds/server"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestDefaultGRPCServer(t *testing.T) {
	if srv := DefaultGRPCServer(); srv != nil {
		t.Fatalf("State has server unexpectedly set. Got %v Want nil", srv)
	}
	testSrv := grpc.NewServer()
	if testSrv == nil {
		t.Fatal("Unable to create a test gRPC server")
	}
	SetDefaultGRPCServer(testSrv)
	assert.Equal(t, testSrv, DefaultGRPCServer(), "gRPC server")
}

func TestConfigFilePath(t *testing.T) {
	SetConfigFilePath("/cfg/cloudprober.cfg")
	assert.Equal(t, "/cfg/cloudprober.cfg", ConfigFilePath(), "config file path")
}

func TestVersion(t *testing.T) {
	if v := Version(); v != "" {
		t.Fatalf("State has version unexpectedly set. Got %v Want \"\"", v)
	}
	SetVersion("v1.0.0")
	assert.Equal(t, "v1.0.0", Version(), "version")
}

func TestBuildTimestamp(t *testing.T) {
	if ts := BuildTimestamp(); !ts.IsZero() {
		t.Fatalf("State has build timestamp unexpectedly set. Got %v Want \"\"", ts)
	}
	ts := time.Now()
	SetBuildTimestamp(ts)
	assert.Equal(t, ts, BuildTimestamp(), "build timestamp")
}

func TestLocalRDSServer(t *testing.T) {
	if rds := LocalRDSServer(); rds != nil {
		t.Fatalf("State has LocalRDSServer unexpectedly set. Got %v Want nil", rds)
	}
}

func TestResetRuntime(t *testing.T) {
	oldVersion, oldTimestamp, oldPath := Version(), BuildTimestamp(), ConfigFilePath()
	t.Cleanup(func() {
		ResetRuntime()
		SetVersion(oldVersion)
		SetBuildTimestamp(oldTimestamp)
		SetConfigFilePath(oldPath)
	})
	timestamp := time.Now()
	SetVersion("reload-test")
	SetBuildTimestamp(timestamp)
	SetConfigFilePath("config.textpb")
	SetDefaultGRPCServer(grpc.NewServer())
	SetLocalRDSServer(&rdsserver.Server{})
	SetDefaultHTTPServeMux(http.NewServeMux())
	assert.NoError(t, AddWebHandler("/old", func(http.ResponseWriter, *http.Request) {}, WithArtifactsLink("/artifact")))
	ResetRuntime()
	assert.Nil(t, DefaultGRPCServer())
	assert.Nil(t, LocalRDSServer())
	assert.Nil(t, DefaultHTTPServeMux())
	assert.Empty(t, AllLinks())
	assert.Empty(t, ArtifactsURLs())
	assert.Equal(t, "reload-test", Version())
	assert.Equal(t, timestamp, BuildTimestamp())
	assert.Equal(t, "config.textpb", ConfigFilePath())
	SetDefaultHTTPServeMux(http.NewServeMux())
	assert.NoError(t, AddWebHandler("/old", func(http.ResponseWriter, *http.Request) {}))
}
