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

package external

import (
	"context"
	"runtime"
	"testing"

	configpb "github.com/cloudprober/cloudprober/internal/servers/external/proto"
	"github.com/cloudprober/cloudprober/logger"
	"google.golang.org/protobuf/proto"
)

func TestCloseWaitsForCommandExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test command requires a POSIX shell")
	}
	s, err := New(context.Background(), &configpb.ServerConf{Command: proto.String("/bin/sh -c 'exec sleep 60'")}, &logger.Logger{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.cmd.ProcessState == nil {
		t.Fatal("Close returned before the command exited")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestEmptyCommand(t *testing.T) {
	if _, err := New(context.Background(), &configpb.ServerConf{}, &logger.Logger{}); err == nil {
		t.Fatal("New accepted an empty command")
	}
}

func TestPrepareReportsExecutionFailure(t *testing.T) {
	s, err := New(context.Background(), &configpb.ServerConf{Command: proto.String("/nonexistent/cloudprober-test-server")}, &logger.Logger{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Prepare(context.Background()); err == nil {
		t.Fatal("Prepare succeeded with a missing executable")
	}
}

func TestStartDoesNotRestartPreparedCommand(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test command requires a POSIX shell")
	}
	s, err := New(context.Background(), &configpb.ServerConf{Command: proto.String("/bin/sh -c 'exec sleep 60'")}, &logger.Logger{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	pid := s.cmd.Process.Pid
	if err := s.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if s.cmd.Process.Pid != pid {
		t.Fatal("Start launched a second command after Prepare")
	}
}
