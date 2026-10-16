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

// Package lifecycle provides cancellable resource-discovery refresh loops.
package lifecycle

import (
	"context"
	"math/rand"
	"time"
)

// Context returns the supplied context or a background context for legacy
// constructors and listers created directly by tests.
func Context(contexts ...context.Context) context.Context {
	if len(contexts) != 0 && contexts[0] != nil {
		return contexts[0]
	}
	return context.Background()
}

// Wait sleeps until the delay expires or the context is cancelled. It reports
// whether the delay elapsed.
func Wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-Context(ctx).Done():
		return false
	case <-timer.C:
		return Context(ctx).Err() == nil
	}
}

// Run performs an initial refresh, then periodically refreshes with randomized
// startup delay. It returns when ctx is cancelled; callbacks must also honor ctx.
func Run(ctx context.Context, interval time.Duration, initial, refresh func()) {
	ctx = Context(ctx)
	if ctx.Err() != nil {
		return
	}
	if initial != nil {
		initial()
	}
	if interval <= 0 || !Wait(ctx, time.Duration(rand.Int63n(int64(interval)))) {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			refresh()
		}
	}
}
