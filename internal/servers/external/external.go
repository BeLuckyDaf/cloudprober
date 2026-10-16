// Copyright 2020 The Cloudprober Authors.
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

// Package external adds support for an external server.
package external

import (
	"context"
	"fmt"
	"os/exec"
	"sync"

	configpb "github.com/cloudprober/cloudprober/internal/servers/external/proto"
	"github.com/cloudprober/cloudprober/logger"
	"github.com/cloudprober/cloudprober/metrics"
	"github.com/google/shlex"
)

// Server implements a external command runner.
type Server struct {
	c *configpb.ServerConf
	l *logger.Logger

	cmdName string
	cmdArgs []string
	cmd     *exec.Cmd
	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	closed  bool
}

// TODO
//	1. Export health status (pid-file OR by monitoring the process started.)
//	2. Add support for command-line substitution.
//  2. Add auto-restart support.

// New creates a new external server.
func New(initCtx context.Context, c *configpb.ServerConf, l *logger.Logger) (*Server, error) {
	cmdParts, err := shlex.Split(c.GetCommand())
	if err != nil {
		return nil, fmt.Errorf("error parsing command line (%s): %v", c.GetCommand(), err)
	}

	if len(cmdParts) == 0 {
		return nil, fmt.Errorf("external server command cannot be empty")
	}
	s := &Server{
		c:       c,
		l:       l,
		cmdName: cmdParts[0],
		cmdArgs: cmdParts[1:],
	}
	context.AfterFunc(initCtx, func() { s.Close() })
	return s, nil
}

// Prepare starts the command during runtime initialization so executable and
// permission errors are reported before a replacement configuration is accepted.
func (s *Server) Prepare(ctx context.Context) error {
	return s.Start(ctx, nil)
}

// Start runs the external command.
func (s *Server) Start(ctx context.Context, dataChan chan<- *metrics.EventMetrics) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return context.Canceled
	}
	if s.done != nil {
		return nil
	}
	ctx, s.cancel = context.WithCancel(ctx)
	s.cmd = exec.CommandContext(ctx, s.cmdName, s.cmdArgs...)
	if err := s.cmd.Start(); err != nil {
		s.cancel()
		return err
	}
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		err := s.cmd.Wait()
		s.l.Infof("Command %s exited. Err status: %v", s.c.GetCommand(), err)
	}()
	return nil
}

// Close stops the command and waits for its resources to be released.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
	done := s.done
	s.mu.Unlock()
	if done != nil {
		<-done
	}
	return nil
}
