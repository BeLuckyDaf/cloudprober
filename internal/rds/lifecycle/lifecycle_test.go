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

package lifecycle

import (
	"context"
	"testing"
	"time"
)

func TestRunStopsDuringInitialDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	initial := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, time.Hour, func() { close(initial) }, func() { t.Error("unexpected refresh after cancellation") })
	}()
	<-initial
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("refresh loop did not stop during startup delay")
	}
}

func TestRunAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	Run(ctx, time.Hour, func() { t.Error("called initial refresh after cancellation") }, func() { t.Error("called refresh after cancellation") })
}

func TestWaitCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Wait(ctx, time.Hour) {
		t.Fatal("wait reported elapsed delay after cancellation")
	}
}
