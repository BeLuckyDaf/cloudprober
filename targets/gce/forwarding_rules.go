// Copyright 2017 The Cloudprober Authors.
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

package gce

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/cloudprober/cloudprober/internal/rds/lifecycle"
	"github.com/cloudprober/cloudprober/logger"
	"github.com/cloudprober/cloudprober/targets/endpoint"
	configpb "github.com/cloudprober/cloudprober/targets/gce/proto"
	compute "google.golang.org/api/compute/v1"
)

// globalForwardingRules is a singleton instance of the forwardingRules struct.
// It is presented as a singleton because, like instances, forwardingRules provides
// a cache layer that is best shared by all probes.
var (
	globalForwardingRules *forwardingRules
	forwardingRulesMu     sync.Mutex
)

// forwardingRules is a lister which lists GCE forwarding rules (see
// https://cloud.google.com/compute/docs/load-balancing/network/forwarding-rules
// for information on forwarding rules). In addition to being able to list the
// rules, this particular lister implements a cache. On a timer (configured by
// GlobalGCETargetsOptions.re_eval_sec cloudprober/targets/targets.proto) in the
// background the cache will be populated (by the equivalent of running "gcloud
// compute forwarding-rules list"). Listing actually only returns the current
// contents of that cache.
//
// Note that because this uses the GCLOUD API, GCE staging is unable to use this
// target type. See b/26320525 for more on this.
//
// TODO(izzycecil): The cache layer provided by this, instances, lameduck, and resolver
//
//	are all pretty similar. RTC will need a similar cache. I should
//	abstract out this whole cache layer. It will be more testable that
//	way, and probably more readable, as well.
type forwardingRules struct {
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.RWMutex
	project     string
	c           *configpb.ForwardingRules
	names       []string
	localRegion string
	globalRule  bool
	cache       map[string]*compute.ForwardingRule
	apiVersion  string
	l           *logger.Logger
}

// List produces a list of all the forwarding rules. The list is similar to
// "gcloud compute forwarding-rules list", but with a cache layer reducing the
// number of actual API calls made.
func (frp *forwardingRules) ListEndpoints() []endpoint.Endpoint {
	frp.mu.RLock()
	defer frp.mu.RUnlock()
	return endpoint.EndpointsFromNames(frp.names)
}

// Resolve returns the IP address associated with the forwarding
// rule. Eventually we can expand this to return protocol and port as well.
func (frp *forwardingRules) Resolve(name string, ipVer int) (net.IP, error) {
	frp.mu.RLock()
	defer frp.mu.RUnlock()
	f := frp.cache[name]
	if f == nil {
		return nil, fmt.Errorf("gce.forwardingRulesProvider.resolve(%s): forwarding rule not in in-memory GCE forwardingRules database", name)
	}
	return net.ParseIP(f.IPAddress), nil
}

// This function will attempt to refresh the cache of GCE targets.
// This attempt may fail, in which case the function will log and leave the cache untouched,
// since it is assumed that the cache will be refreshed by a subsequent call.
// It runs API calls equivalent to "gcloud compute forwarding-rules list"
func (frp *forwardingRules) expand() {
	frp.l.Infof("gce.forwardingRules.expand: expanding GCE targets")

	cs, err := defaultComputeService(frp.apiVersion, frp.ctx)
	if err != nil {
		frp.l.Errorf("gce.forwardingRules.expand: error while creating the compute service: %v", err)
		return
	}
	var forwardingRulesList []*compute.ForwardingRule
	if frp.globalRule {
		l, err := cs.GlobalForwardingRules.List(frp.project).Context(lifecycle.Context(frp.ctx)).Do()
		if err != nil {
			frp.l.Errorf("gce.forwardingRules.expand(global): error while getting the list of global forwarding rules: %v", err)
			return
		}
		forwardingRulesList = append(forwardingRulesList, l.Items...)
	} else {
		regions, err := frp.getTargetRegions(cs)
		if err != nil {
			frp.l.Errorf("gce.forwardingRules.expand: error while getting the list of target regions: %v", err)
			return
		}

		for _, region := range regions {
			l, err := cs.ForwardingRules.List(frp.project, region).Context(lifecycle.Context(frp.ctx)).Do()
			if err != nil {
				frp.l.Errorf("gce.forwardingRules.expand(region=%s): error while getting the list of forwarding rules: %v", region, err)
				return
			}
			forwardingRulesList = append(forwardingRulesList, l.Items...)
		}
	}
	var result []string
	cache := make(map[string]*compute.ForwardingRule)
	for _, ins := range forwardingRulesList {
		cache[ins.Name] = ins
		result = append(result, ins.Name)
	}

	frp.l.Debugf("Expanded target list: %q", result)
	frp.mu.Lock()
	frp.cache = cache
	frp.names = result
	frp.mu.Unlock()
}

// getTargetRegions returns the list of regions we are interested in based on
// the configuration.
func (frp *forwardingRules) getTargetRegions(cs *compute.Service) ([]string, error) {
	// Select local region if region is not specified.
	if len(frp.c.GetRegion()) == 0 {
		return []string{frp.localRegion}, nil
	}

	// If more than one region is specified or only specified region is not "all"
	if len(frp.c.GetRegion()) > 1 || frp.c.GetRegion()[0] != "all" {
		return frp.c.GetRegion(), nil
	}

	l, err := cs.Regions.List(frp.project).Context(lifecycle.Context(frp.ctx)).Do()
	if err != nil {
		return nil, err
	}

	regions := make([]string, len(l.Items))
	for i := range l.Items {
		regions[i] = l.Items[i].Name
	}

	return regions, nil
}

// Instance's region is not stored in the metadata, we need to get it from the zone.
func getLocalRegion() (string, error) {
	if !metadata.OnGCE() {
		return "", errors.New("getLocalRegion: not running on GCE")
	}

	zone, err := metadata.Zone()
	if err != nil {
		return "", err
	}

	zoneParts := strings.Split(zone, "-")
	return strings.Join(zoneParts[0:len(zoneParts)-1], "-"), nil
}

// newForwardingrules will (if needed) initialize and return the
// globalForwardingRules singleton.
func newForwardingRules(project string, opts *configpb.GlobalOptions, frpb *configpb.ForwardingRules, l *logger.Logger, contexts ...context.Context) (*forwardingRules, error) {
	forwardingRulesMu.Lock()
	defer forwardingRulesMu.Unlock()
	if globalForwardingRules != nil {
		return globalForwardingRules, nil
	}
	reEvalInterval := time.Duration(opts.GetReEvalSec()) * time.Second
	if reEvalInterval <= 0 {
		return nil, fmt.Errorf("gce.forwarding_rules: re_eval_sec must be positive")
	}
	var localRegion string
	if !frpb.GetGlobalRule() && len(frpb.GetRegion()) == 0 {
		var err error
		localRegion, err = getLocalRegion()
		if err != nil {
			return nil, fmt.Errorf("gce.newForwardingRules: error while getting local region: %v", err)
		}
		l.Infof("gce.newForwardingRules: local region: %s", localRegion)
	}
	ctx, cancel := context.WithCancel(lifecycle.Context(contexts...))
	frp := &forwardingRules{
		ctx:         ctx,
		cancel:      cancel,
		project:     project,
		c:           frpb,
		localRegion: localRegion,
		globalRule:  frpb.GetGlobalRule(),
		cache:       make(map[string]*compute.ForwardingRule),
		apiVersion:  opts.GetApiVersion(),
		l:           l,
	}
	globalForwardingRules = frp
	go lifecycle.Run(ctx, reEvalInterval, frp.expand, frp.expand)
	return frp, nil
}
