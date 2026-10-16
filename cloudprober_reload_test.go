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

package cloudprober

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudprober/cloudprober/config"
	pb "github.com/cloudprober/cloudprober/prober/proto"
	probepb "github.com/cloudprober/cloudprober/probes/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

func reloadTestClient(t *testing.T, port int32, creds credentials.TransportCredentials) pb.CloudproberClient {
	t.Helper()
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(creds))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return pb.NewCloudproberClient(conn)
}

func reloadTestCall(t *testing.T, client pb.CloudproberClient) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := client.ReloadConfig(ctx, &pb.ReloadConfigRequest{})
	return err
}

func reloadTestList(t *testing.T, client pb.CloudproberClient) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.ListProbes(ctx, &pb.ListProbesRequest{})
	require.NoError(t, err)
	var names []string
	for _, p := range resp.GetProbe() {
		names = append(names, p.GetName())
	}
	return names
}

func reloadTestHTTP(t *testing.T, port int32, path string) (int, string) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d%s", port, path))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func reloadTestStart(t *testing.T, raw string) string {
	t.Helper()
	resetCloudProber(t)
	t.Cleanup(func() { resetCloudProber(t) })
	previousDisableSysMetrics := *disableSysMetrics
	*disableSysMetrics = true
	t.Cleanup(func() { *disableSysMetrics = previousDisableSysMetrics })
	path := filepath.Join(t.TempDir(), "cloudprober.cfg")
	require.NoError(t, os.WriteFile(path, []byte(raw), 0600))
	require.NoError(t, InitWithConfigSource(config.ConfigSourceWithFile(path)))
	Start(context.Background())
	return path
}

func TestReloadConfigAllSettings(t *testing.T) {
	ports := freePortsT(t, 5)
	rawConfig := func(httpPort, grpcPort int32, name, metricsURL string) string {
		return fmt.Sprintf(`host: "127.0.0.1"
port: %d
grpc_port: %d
disable_jitter: true
sysvars_interval_msec: 20
server { type: HTTP http_server { port: %d response_header { key: "Generation" value: %q } } }
probe { name: %q type: HTTP interval: "20ms" timeout: "10ms"
    targets { host_names: "127.0.0.1" } http_probe { port: %d } }
surfacer { type: PROMETHEUS prometheus_surfacer { metrics_url: %q } }
`, httpPort, grpcPort, ports[4], name, name, ports[4], metricsURL)
	}
	first := rawConfig(ports[0], ports[1], "before", "/metrics")
	path := reloadTestStart(t, first)
	client := reloadTestClient(t, ports[1], insecure.NewCredentials())
	readerCtx, stopReaders := context.WithCancel(context.Background())
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for readerCtx.Err() == nil {
			GetConfig()
			GetRawConfig()
			GetParsedConfig()
			GetInfo()
		}
	}()
	defer func() { stopReaders(); readers.Wait() }()
	require.Equal(t, []string{"before"}, reloadTestList(t, client))
	old := GetProber()

	// Replace probes, the metrics exporter, both default listeners, and an
	// additional HTTP server on the same port in one real RPC.
	second := rawConfig(ports[2], ports[3], "after", "/new-metrics")
	require.NoError(t, os.WriteFile(path, []byte(second), 0600))
	require.NoError(t, reloadTestCall(t, client))
	require.NotSame(t, old, GetProber())
	nextClient := reloadTestClient(t, ports[3], insecure.NewCredentials())
	require.Equal(t, []string{"after"}, reloadTestList(t, nextClient))
	require.Equal(t, strings.TrimSuffix(second, "\n"), GetRawConfig())
	require.Equal(t, strings.TrimSuffix(second, "\n"), GetParsedConfig())
	require.Equal(t, ports[2], GetConfig().GetPort())
	code, _ := reloadTestHTTP(t, ports[2], "/health")
	require.Equal(t, http.StatusOK, code)
	code, _ = reloadTestHTTP(t, ports[2], "/new-metrics")
	require.Equal(t, http.StatusOK, code)
	code, _ = reloadTestHTTP(t, ports[2], "/metrics")
	require.Equal(t, http.StatusNotFound, code)
	for _, port := range ports[:2] {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		require.NoError(t, err, "old listener must be released")
		ln.Close()
	}

	// Repeated reloads must also work without changing any listener ports.
	for i := 0; i < 3; i++ {
		require.NoError(t, reloadTestCall(t, nextClient))
		nextClient = reloadTestClient(t, ports[3], insecure.NewCredentials())
		require.Equal(t, []string{"after"}, reloadTestList(t, nextClient))
	}
}

func TestReloadConfigFailureRestoresPreviousConfig(t *testing.T) {
	ports := freePortsT(t, 2)
	raw := fmt.Sprintf(`host: "127.0.0.1" port: %d grpc_port: %d
        disable_jitter: true surfacer {}
        probe { name: "original" type: HTTP targets { host_names: "localhost" } }
    `, ports[0], ports[1])
	path := reloadTestStart(t, raw)
	client := reloadTestClient(t, ports[1], insecure.NewCredentials())
	original := GetProber()

	for _, invalid := range []string{"not valid protobuf {", strings.Replace(raw, "type: HTTP", "type: INVALID", 1)} {
		require.NoError(t, os.WriteFile(path, []byte(invalid), 0600))
		require.Equal(t, codes.InvalidArgument, status.Code(reloadTestCall(t, client)))
		require.Same(t, original, GetProber(), "parse errors must not stop the active runtime")
		require.Equal(t, raw, GetRawConfig())
		require.Equal(t, []string{"original"}, reloadTestList(t, client))
	}

	// Rollback also preserves successful API mutations made since startup.
	dynamic := &probepb.ProbeDef{}
	require.NoError(t, prototext.Unmarshal([]byte(`name: "dynamic" type: HTTP targets { host_names: "localhost" }`), dynamic))
	_, err := client.AddProbe(context.Background(), &pb.AddProbeRequest{ProbeConfig: dynamic})
	require.NoError(t, err)
	_, err = client.RemoveProbe(context.Background(), &pb.RemoveProbeRequest{ProbeName: proto.String("original")})
	require.NoError(t, err)

	// Runtime failures must release acquired resources and restore the old
	// runtime, including failures that used to surface only in Start goroutines.
	for _, extra := range []string{
		`probe { name: "invalid" type: HTTP targets { shared_targets: "missing" } }`,
		`surfacer { type: PROMETHEUS prometheus_surfacer { metrics_url: "/health" } }`,
		`server { type: HTTP http_server { port: 0 pattern_data_handler { response_size: -1 } } }`,
		`server { type: EXTERNAL external_server { command: "/cloudprober-nonexistent-server" } }`,
	} {
		require.NoError(t, os.WriteFile(path, []byte(raw+extra), 0600))
		err := reloadTestCall(t, client)
		require.Equal(t, codes.InvalidArgument, status.Code(err), extra)
		require.Contains(t, err.Error(), "previous configuration restored")
		client = reloadTestClient(t, ports[1], insecure.NewCredentials())
		require.Equal(t, []string{"dynamic"}, reloadTestList(t, client))
		require.Equal(t, raw, GetRawConfig())
		require.Equal(t, raw, GetParsedConfig())
	}

	// A failed reload does not poison the source for the next attempt.
	require.NoError(t, os.WriteFile(path, []byte(strings.Replace(raw, "original", "recovered", 1)), 0600))
	require.NoError(t, reloadTestCall(t, client))
	client = reloadTestClient(t, ports[1], insecure.NewCredentials())
	require.Equal(t, []string{"recovered"}, reloadTestList(t, client))
}

func TestReloadConfigTLS(t *testing.T) {
	ports := freePortsT(t, 2)
	raw := fmt.Sprintf(`host: "127.0.0.1" port: %d grpc_port: %d surfacer {}`, ports[0], ports[1])
	path := reloadTestStart(t, raw)
	client := reloadTestClient(t, ports[1], insecure.NewCredentials())
	dir := t.TempDir()
	certFile, keyFile, roots := reloadTestCertificate(t, dir)
	tlsRaw := raw + fmt.Sprintf(` grpc_tls_config { tls_cert_file: %q tls_key_file: %q }`, certFile, keyFile)
	require.NoError(t, os.WriteFile(path, []byte(tlsRaw), 0600))
	// This response still traverses the retiring plaintext connection.
	require.NoError(t, reloadTestCall(t, client))
	tlsClient := reloadTestClient(t, ports[1], credentials.NewTLS(&tls.Config{RootCAs: roots}))
	require.Empty(t, reloadTestList(t, tlsClient))
	require.NoError(t, reloadTestCall(t, tlsClient))
	tlsClient = reloadTestClient(t, ports[1], credentials.NewTLS(&tls.Config{RootCAs: roots}))
	require.Empty(t, reloadTestList(t, tlsClient))
	// Removing TLS takes effect on the same port as well.
	require.NoError(t, os.WriteFile(path, []byte(raw), 0600))
	require.NoError(t, reloadTestCall(t, tlsClient))
	client = reloadTestClient(t, ports[1], insecure.NewCredentials())
	require.Empty(t, reloadTestList(t, client))
}

func reloadTestCertificate(t *testing.T, dir string) (string, string, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600))
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(certPEM))
	return certFile, keyFile, roots
}

func TestReloadConfigPreconditions(t *testing.T) {
	resetCloudProber(t)
	require.Equal(t, codes.FailedPrecondition, status.Code(ReloadConfig(context.Background())))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Equal(t, codes.Canceled, status.Code(ReloadConfig(ctx)))
}

func TestReloadConfigListenerConflictAndLifetime(t *testing.T) {
	ports := freePortsT(t, 2)
	raw := fmt.Sprintf(`host: "127.0.0.1" port: %d grpc_port: %d surfacer {}`, ports[0], ports[1])
	resetCloudProber(t)
	t.Cleanup(func() { resetCloudProber(t) })
	path := filepath.Join(t.TempDir(), "config.cfg")
	require.NoError(t, os.WriteFile(path, []byte(raw), 0600))
	require.NoError(t, InitWithConfigSource(config.ConfigSourceWithFile(path)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	Start(ctx)
	client := reloadTestClient(t, ports[1], insecure.NewCredentials())
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()
	invalid := strings.Replace(raw, fmt.Sprintf("port: %d", ports[0]),
		fmt.Sprintf("port: %d", occupied.Addr().(*net.TCPAddr).Port), 1)
	require.NoError(t, os.WriteFile(path, []byte(invalid), 0600))
	err = reloadTestCall(t, client)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "previous configuration restored")
	require.Equal(t, raw, GetRawConfig())
	client = reloadTestClient(t, ports[1], insecure.NewCredentials())
	reloadTestList(t, client)
	require.NoError(t, os.WriteFile(path, []byte(raw), 0600))
	require.NoError(t, reloadTestCall(t, client))
	cancel()
	require.Eventually(t, func() bool { return GetProber() == nil }, 5*time.Second, time.Millisecond)
	// Shutdown clears the public instance before releasing its listeners.
	require.Eventually(t, func() bool {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[1]))
		if err != nil {
			return false
		}
		ln.Close()
		return true
	}, 5*time.Second, time.Millisecond)
}

func TestInitFailureCanRetry(t *testing.T) {
	resetCloudProber(t)
	t.Cleanup(func() { resetCloudProber(t) })
	ports := freePortsT(t, 2)
	raw := fmt.Sprintf(`host: "127.0.0.1" port: %d grpc_port: %d surfacer {}`, ports[0], ports[1])
	path := filepath.Join(t.TempDir(), "config.cfg")
	require.NoError(t, os.WriteFile(path, []byte(raw+` probe { name: "invalid" type: HTTP targets { shared_targets: "missing" } }`), 0600))
	source := config.ConfigSourceWithFile(path)
	require.Error(t, InitWithConfigSource(source))
	require.NoError(t, os.WriteFile(path, []byte(raw), 0600))
	require.NoError(t, InitWithConfigSource(source))
	Start(context.Background())
	client := reloadTestClient(t, ports[1], insecure.NewCredentials())
	reloadTestList(t, client)
	require.NoError(t, reloadTestCall(t, client))
}
