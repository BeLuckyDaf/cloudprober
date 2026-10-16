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

package file

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	configpb "github.com/cloudprober/cloudprober/internal/surfacers/file/proto"
	"github.com/cloudprober/cloudprober/logger"
	"github.com/cloudprober/cloudprober/metrics"
	"github.com/cloudprober/cloudprober/surfacers/options"
	"google.golang.org/protobuf/proto"
)

func TestCloseKeepsStdoutOpen(t *testing.T) {
	s, err := New(context.Background(), &configpb.SurfacerConf{}, &options.Options{MetricsBufferSize: 10}, &logger.Logger{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stdout.Stat(); err != nil {
		t.Fatalf("Close closed process stdout: %v", err)
	}
}

func TestCloseDuringWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := New(ctx, &configpb.SurfacerConf{FilePath: proto.String(filepath.Join(t.TempDir(), "metrics")), CompressionEnabled: proto.Bool(true)}, &options.Options{MetricsBufferSize: 1000}, &logger.Logger{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			s.Write(ctx, metrics.NewEventMetrics(time.Now()).AddMetric("total", metrics.NewInt(1)))
		}
	}()
	cancel()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if _, err := s.outf.Stat(); err == nil {
		t.Fatal("Close left the output file open")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestWithAppendPreservesExistingOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics")
	const previous = "metrics from the previous runtime\n"
	if err := os.WriteFile(path, []byte(previous), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := New(WithAppend(context.Background()), &configpb.SurfacerConf{FilePath: proto.String(path)}, &options.Options{MetricsBufferSize: 10}, &logger.Logger{})
	if err != nil {
		t.Fatal(err)
	}
	// Even a candidate that fails before writing metrics must preserve output.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != previous {
		t.Fatalf("reload erased existing metrics: %q", got)
	}
}
