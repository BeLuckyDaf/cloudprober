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

package prometheus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cloudprober/cloudprober/logger"
	"github.com/cloudprober/cloudprober/state"
	"github.com/cloudprober/cloudprober/surfacers/options"
)

func TestRetiredHandlerReturnsServiceUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	state.SetDefaultHTTPServeMux(mux)
	s, err := New(context.Background(), nil, &options.Options{}, &logger.Logger{})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	done := make(chan struct{})
	response := httptest.NewRecorder()
	go func() { defer close(done); mux.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil)) }()
	select {
	case <-done:
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", response.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("request hung after exporter retired")
	}
}
