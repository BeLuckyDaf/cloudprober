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

// Package response buffers asynchronous surfacer responses so a canceled HTTP
// handler never races a worker still writing the response.
package response

import (
	"bytes"
	"net/http"
)

// Buffer implements http.ResponseWriter without a live client connection.
// Access it only from the worker until that worker signals completion.
type Buffer struct {
	body   bytes.Buffer
	header http.Header
	status int
}

func New() *Buffer                    { return &Buffer{header: make(http.Header)} }
func (b *Buffer) Header() http.Header { return b.header }
func (b *Buffer) WriteHeader(status int) {
	if b.status == 0 {
		b.status = status
	}
}
func (b *Buffer) Write(data []byte) (int, error) {
	b.WriteHeader(http.StatusOK)
	return b.body.Write(data)
}

// WriteTo sends a completed response to the client.
func (b *Buffer) WriteTo(w http.ResponseWriter) {
	for name, values := range b.header {
		w.Header()[name] = values
	}
	if b.status != 0 {
		w.WriteHeader(b.status)
	}
	w.Write(b.body.Bytes())
}
