// Copyright 2017-2019 The Cloudprober Authors.
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

/*
Package cloudprober provides a prober for running a set of probes.

Cloudprober takes in a config proto which dictates what probes should be created
with what configuration, and manages the asynchronous fan-in/fan-out of the
metrics data from these probes.
*/
package cloudprober

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudprober/cloudprober/common/tlsconfig"
	"github.com/cloudprober/cloudprober/config"
	configpb "github.com/cloudprober/cloudprober/config/proto"
	"github.com/cloudprober/cloudprober/internal/alerting"
	"github.com/cloudprober/cloudprober/internal/reaper"
	"github.com/cloudprober/cloudprober/internal/servers"
	serverspb "github.com/cloudprober/cloudprober/internal/servers/proto"
	filesurfacer "github.com/cloudprober/cloudprober/internal/surfacers/file"
	"github.com/cloudprober/cloudprober/internal/sysvars"
	"github.com/cloudprober/cloudprober/internal/tracing"
	"github.com/cloudprober/cloudprober/logger"
	"github.com/cloudprober/cloudprober/logger/logstore"
	"github.com/cloudprober/cloudprober/metrics/singlerun"
	"github.com/cloudprober/cloudprober/prober"
	"github.com/cloudprober/cloudprober/probes"
	probes_configpb "github.com/cloudprober/cloudprober/probes/proto"
	system_configpb "github.com/cloudprober/cloudprober/probes/system/proto"
	"github.com/cloudprober/cloudprober/state"
	"github.com/cloudprober/cloudprober/surfacers"
	"github.com/cloudprober/cloudprober/targets"
	"github.com/cloudprober/cloudprober/web"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/channelz/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	sysvarsModuleName = "sysvars"

	// tracingShutdownTimeout is how long we wait for buffered trace spans to
	// be flushed on shutdown. It's kept below the default stop_time_sec (5s),
	// so that we get to log the failure if the flush doesn't finish in time.
	tracingShutdownTimeout = 3 * time.Second
)

// Constants defining the default server host and port.
const (
	DefaultServerHost   = ""
	DefaultServerPort   = 9313
	NoGRPCPort          = "NO_GRPC_SERVER" // By default, no gRPC server is started.
	ServerHostEnvVar    = "CLOUDPROBER_HOST"
	ServerPortEnvVar    = "CLOUDPROBER_PORT"
	GRPCPortEnvVar      = "CLOUDPROBER_GRPC_PORT"
	DisableHTTPDebugVar = "CLOUDPROBER_DISABLE_HTTP_PPROF"
)

var (
	disableSysMetrics = flag.Bool("disable_sys_metrics", false, "Disable system metrics probe")
	logStoreMaxMemMB  = flag.Int("log_store_max_mem_mb", 50, "Max memory in MB for in-memory log store. 0 to disable.")
	logStoreMinLevel  = flag.String("log_store_min_level", "INFO", "Minimum log level for stored logs. Valid values: DEBUG, INFO, WARNING, ERROR")
)

// instance is everything that Init acquires, kept in one struct so that
// Shutdown can drop all of it in one go.
type instance struct {
	prober          *prober.Prober
	defaultServerLn net.Listener
	defaultGRPCLn   net.Listener
	httpSrv         *http.Server
	configSource    config.ConfigSource
	config          *configpb.ProberConfig
	cancelStartCtx  context.CancelFunc
	cancelInitCtx   context.CancelFunc
	tracingShutdown tracing.ShutdownFunc
	grpcSrv         *grpc.Server
	customGRPC      bool
	rawConfig       string
	parsedConfig    string
	lifetimeCtx     context.Context
	cancelLifetime  context.CancelFunc
	stopWatch       func() bool
}

// lifecycleMu serializes initialization, reload, and shutdown without blocking
// config readers while workers finish or exporters flush.
var lifecycleMu sync.Mutex

// Global prober.Prober instance protected by a mutex. Cloudprober is a
// process-wide singleton: it's initialized once and, once shut down, stays
// down -- 'done' is what makes that stick.
var cloudProber struct {
	instance
	done bool
	sync.RWMutex
}

func getServerHost(c *configpb.ProberConfig) string {
	serverHost := c.GetHost()
	if serverHost == "" {
		serverHost = DefaultServerHost
		// If ServerHostEnvVar is defined, it will override the default
		// server host.
		if host := os.Getenv(ServerHostEnvVar); host != "" {
			serverHost = host
		}
	}
	return serverHost
}

// 'grpc_port' from config takes precedence over environment variable, if both are set.
func getGRPCPort(c *configpb.ProberConfig) string {
	grpcPort := c.GetGrpcPort()
	if grpcPort != 0 {
		return strconv.Itoa(int(grpcPort))
	} else if envPort := os.Getenv(GRPCPortEnvVar); envPort != "" {
		return envPort
	}
	return NoGRPCPort
}

func getDefaultServerPort(c *configpb.ProberConfig, l *logger.Logger) (int, error) {
	if c.GetPort() != 0 {
		return int(c.GetPort()), nil
	}

	// If ServerPortEnvVar is defined, it will override the default
	// server port.
	portStr := os.Getenv(ServerPortEnvVar)
	if portStr == "" {
		return DefaultServerPort, nil
	}

	if strings.HasPrefix(portStr, "tcp://") {
		l.Warningf("%s environment variable likely set by Kubernetes (to %s), ignoring it", ServerPortEnvVar, portStr)
		return DefaultServerPort, nil
	}

	port, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse default port from the env var: %s=%s", ServerPortEnvVar, portStr)
	}

	return int(port), nil
}

func initDefaultServer(c *configpb.ProberConfig, l *logger.Logger) (net.Listener, error) {
	serverHost := getServerHost(c)
	serverPort, err := getDefaultServerPort(c, l)

	if err != nil {
		return nil, err
	}

	ln, err := net.Listen("tcp", net.JoinHostPort(serverHost, strconv.Itoa(serverPort)))
	if err != nil {
		return nil, fmt.Errorf("error while creating listener for default HTTP server: %v", err)
	}

	return ln, nil
}

func initDefaultGRPCServer(c *configpb.ProberConfig) (net.Listener, error) {
	grpcPort := getGRPCPort(c)
	if grpcPort == NoGRPCPort {
		return nil, nil
	}
	serverHost := getServerHost(c)

	grpcLn, err := net.Listen("tcp", net.JoinHostPort(serverHost, grpcPort))
	if err != nil {
		return nil, fmt.Errorf("error while creating listener for default gRPC server: %v", err)
	}

	s := state.DefaultGRPCServer()
	if s == nil {
		// Create the default gRPC server now, so that other modules can register
		// their services with it in the prober.Init() phase.
		var serverOpts []grpc.ServerOption

		if c.GetGrpcTlsConfig() != nil {
			tlsConfig, err := tlsconfig.FromProto(c.GetGrpcTlsConfig())
			if err != nil {
				grpcLn.Close()
				return nil, err
			}
			tlsConfig.ClientCAs = tlsConfig.RootCAs
			serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
		}

		s = grpc.NewServer(serverOpts...)
		state.SetDefaultGRPCServer(s)
	}
	reflection.Register(s)
	// register channelz service to the default grpc server port
	service.RegisterChannelzServiceToServer(s)

	return grpcLn, nil
}

func setDebugHandlers(srvMux *http.ServeMux) {
	if os.Getenv(DisableHTTPDebugVar) != "" {
		return
	}
	srvMux.HandleFunc("/debug/pprof/", pprof.Index)
	srvMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	srvMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	srvMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	srvMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

// InitFromConfig initializes Cloudprober using the provided config.
// Deprecated: This function is kept only for compatibility reasons. It's
// recommended to use Init() or InitWithConfigSource() instead.
func InitFromConfig(configFile string) error {
	return InitWithConfigSource(config.ConfigSourceWithFile(configFile))
}

// Init initializes Cloudprober using the default config source.
//
// Optionally, one can use the 'state' package to customize cloudprober; for example,
// by providing a custom gRPC server, before calling this func to initialize the cloudprober.
//
// Cloudprober is a process-wide singleton, so Init can be called only once:
// calling it again, or after Shutdown, panics.
func Init() error {
	return InitWithConfigSource(config.DefaultConfigSource())
}

func InitWithConfigSource(configSrc config.ConfigSource) error {
	return initWithConfigSource(configSrc)
}

func initWithConfigSource(configSrc config.ConfigSource) error {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	cloudProber.Lock()
	defer cloudProber.Unlock()

	// Cloudprober is a singleton and Shutdown is one-way, so initializing
	// again is a programming error, not something to paper over.
	if cloudProber.prober != nil || cloudProber.done {
		panic("cloudprober is already initialized; it cannot be initialized again")
	}

	// Initialize sysvars module
	if err := sysvars.Init(logger.NewWithAttrs(slog.String("component", sysvarsModuleName)), nil); err != nil {
		return err
	}

	cfg, err := configSrc.GetConfig()
	if err != nil {
		return err
	}

	managedGRPC := state.DefaultGRPCServer() == nil
	inst, err := newInstance(configSrc, withSystemProbe(cfg), false)
	if err != nil {
		if managedGRPC {
			state.ResetRuntime()
			targets.ResetRuntime()
		}
		return err
	}
	cloudProber.instance = inst
	return nil
}

// withSystemProbe applies startup defaults to a newly read configuration.
// Rollback uses the actual retired probe set and must not add removed probes.
func withSystemProbe(cfg *configpb.ProberConfig) *configpb.ProberConfig {
	if cfg == nil {
		return nil
	}
	cfg = proto.Clone(cfg).(*configpb.ProberConfig)

	if runtime.GOOS == "linux" {
		// Be careful about imports, we want to check if system probe is configured.
		// We iterate over probes to see if any of them is a system probe.
		sysProbeConfigured := false
		for _, p := range cfg.GetProbe() {
			if p.GetType() == probes_configpb.ProbeDef_SYSTEM {
				sysProbeConfigured = true
				break
			}
		}

		if !*disableSysMetrics && !sysProbeConfigured {
			// Add default system probe
			cfg.Probe = append(cfg.Probe, &probes_configpb.ProbeDef{
				Name:     proto.String("sys_metrics"),
				Type:     probes_configpb.ProbeDef_SYSTEM.Enum(),
				Interval: proto.String("10s"),
				Timeout:  proto.String("5s"),
				Probe: &probes_configpb.ProbeDef_SystemProbe{
					SystemProbe: &system_configpb.ProbeConf{},
				},
			})
		}
	}

	return cfg
}

// newInstance acquires a complete runtime. Its caller serializes access to the
// process-wide module registries and stops the previous runtime first.
func newInstance(configSrc config.ConfigSource, cfg *configpb.ProberConfig, reloading bool) (instance, error) {
	if cfg == nil {
		return instance{}, errors.New("configuration is nil")
	}
	cfg = proto.Clone(cfg).(*configpb.ProberConfig)
	if err := proto.CheckInitialized(cfg); err != nil {
		return instance{}, err
	}
	if cfg.GetSysvarsIntervalMsec() <= 0 {
		return instance{}, errors.New("sysvars_interval_msec must be positive")
	}
	// RegisterService terminates the process on duplicate services. Reject
	// duplicate use of the default gRPC probe-target service before registration.
	sharedGRPC := false
	for _, server := range cfg.GetServer() {
		if server.GetType() == serverspb.ServerDef_GRPC && !server.GetGrpcServer().GetUseDedicatedServer() {
			if sharedGRPC {
				return instance{}, errors.New("only one gRPC target server may use the default gRPC server")
			}
			sharedGRPC = true
		}
	}

	globalLogger := logger.NewWithAttrs(slog.String("component", "global"))

	// cleanup accumulates rollback actions for resources acquired below, run
	// in reverse (LIFO) order if a later step fails. It's cleared just
	// before a successful return, once ownership transfers to cloudProber.
	var cleanup []func()
	defer func() {
		for i := len(cleanup) - 1; i >= 0; i-- {
			cleanup[i]()
		}
	}()

	// Start default HTTP server. It's used for profile handlers and
	// prometheus exporter.
	ln, err := initDefaultServer(cfg, globalLogger)
	if err != nil {
		return instance{}, err
	}
	cleanup = append(cleanup, func() { ln.Close() })
	srvMux := http.NewServeMux()
	setDebugHandlers(srvMux)
	state.SetDefaultHTTPServeMux(srvMux)

	// Start default gRPC server (if configured - result may be nil).
	customGRPC := state.DefaultGRPCServer() != nil
	grpcLn, err := initDefaultGRPCServer(cfg)
	if err != nil {
		return instance{}, err
	}
	if grpcLn != nil {
		cleanup = append(cleanup, func() { grpcLn.Close() })
	}
	if grpcSrv := state.DefaultGRPCServer(); grpcSrv != nil && !customGRPC {
		cleanup = append(cleanup, grpcSrv.Stop)
	}

	// Preserve retained logs across runtime replacements.
	if *logStoreMaxMemMB > 0 && logger.DefaultLogStore() == nil {
		ls := logstore.New(int64(*logStoreMaxMemMB)<<20, logger.ParseLogLevel(*logStoreMinLevel))
		logger.SetDefaultLogStore(ls)
	}

	var tracingShutdown tracing.ShutdownFunc
	if cfg.GetTracing() != nil {
		tl := logger.NewWithAttrs(slog.String("component", "tracing"))
		tracingShutdown, err = tracing.Init(context.Background(), cfg.GetTracing(), tl)
		if err != nil {
			return instance{}, fmt.Errorf("error while initializing tracing: %v", err)
		}
		cleanup = append(cleanup, func() { shutdownTracing(tracingShutdown) })
	} else if reloading {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	}

	// initCtx is used to clean up in case of partial initialization failures. For
	// example, user-configured servers open listeners during initialization and
	// if initialization fails at a later stage, say in probers or surfacers,
	// pr.Init returns an error and we cancel the initCtx, which makes servers
	// close their listeners.
	// TODO(manugarg): Plumb init context from cmd/cloudprober.
	initCtx, cancelFunc := context.WithCancel(context.Background())
	if reloading {
		initCtx = filesurfacer.WithAppend(initCtx)
	}
	cleanup = append(cleanup, cancelFunc)
	pr, err := prober.Init(initCtx, cfg, globalLogger)
	if err != nil {
		return instance{}, err
	}

	inst := instance{
		prober:          pr,
		config:          cfg,
		configSource:    configSrc,
		defaultServerLn: ln,
		defaultGRPCLn:   grpcLn,
		cancelInitCtx:   cancelFunc,
		tracingShutdown: tracingShutdown,
		grpcSrv:         state.DefaultGRPCServer(),
		customGRPC:      customGRPC,
		rawConfig:       configSrc.RawConfig(),
		parsedConfig:    configSrc.ParsedConfig(),
	}

	cleanup = append(cleanup, pr.Stop)
	pr.SetReloadConfigFunc(func(ctx context.Context) error {
		return reloadConfig(ctx, pr)
	})
	if err := web.InitWithDataFuncs(web.DataFuncs{
		GetRawConfig: GetRawConfig, GetParsedConfig: GetParsedConfig, GetInfo: GetInfo,
	}); err != nil {
		return instance{}, err
	}

	if err := state.AddWebHandler("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "OK")
	}); err != nil {
		return instance{}, err
	}

	// Ownership of all acquired resources has transferred to cloudProber;
	// skip the rollback.
	cleanup = nil
	return inst, nil
}

// RunOnce runs requested probes once and print probe results to stdout.
func RunOnce(ctx context.Context, names, format, indent string) error {
	cloudProber.RLock()
	defer cloudProber.RUnlock()

	if cloudProber.prober == nil {
		return errors.New("cloudprober is not initialized")
	}

	var probeNames []string
	// avoid getting '[""]' as probe names
	if names != "" {
		probeNames = strings.Split(names, ",")
	}
	prrs, err := cloudProber.prober.Run(ctx, probeNames)
	fmt.Println(singlerun.FormatProbeRunResults(prrs, singlerun.Format(format), indent))

	// In CLI case, aggregate the probe run errors, so we can more easily show to users.
	for _, prr := range prrs {
		for _, r := range prr {
			err = errors.Join(err, r.Error)
		}
	}

	return err
}

// Start starts a previously initialized Cloudprober.
//
// Note for embedders: if the process is init in its PID namespace (or a child
// subreaper), Start also begins reaping orphaned child processes, since
// nothing else will. That includes zombie children of the embedding binary
// itself, if it leaves any unwaited-for for more than a few minutes.
func Start(ctx context.Context) {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	cloudProber.Lock()
	defer cloudProber.Unlock()
	if cloudProber.prober == nil {
		panic("Prober is not initialized. Did you call cloudprober.Init first?")
	}
	if cloudProber.lifetimeCtx != nil {
		panic("cloudprober is already started")
	}
	lifetimeCtx, cancel := context.WithCancel(ctx)
	cloudProber.lifetimeCtx = lifetimeCtx
	cloudProber.cancelLifetime = cancel
	cloudProber.stopWatch = context.AfterFunc(lifetimeCtx, Shutdown)
	reaper.Start(lifetimeCtx, logger.NewWithAttrs(slog.String("component", "reaper")))
	cloudProber.instance.start()
}

func (inst *instance) start() {
	startCtx, cancel := context.WithCancel(inst.lifetimeCtx)
	inst.cancelStartCtx = cancel
	srvMux := state.DefaultHTTPServeMux()
	inst.httpSrv = &http.Server{Handler: srvMux}
	// Initialize the probe runtime before accepting management requests.
	inst.prober.Start(startCtx)
	go inst.httpSrv.Serve(inst.defaultServerLn)
	if inst.grpcSrv != nil && inst.defaultGRPCLn != nil {
		go inst.grpcSrv.Serve(inst.defaultGRPCLn)
	}
}

// stop releases a runtime's resources. On reload, the caller retires the gRPC
// server separately because the ReloadConfig call itself is still using it.
func (inst *instance) stop(reloading bool) {
	if inst.cancelStartCtx != nil {
		inst.cancelStartCtx()
	}
	if inst.cancelInitCtx != nil {
		inst.cancelInitCtx()
	}
	if inst.prober != nil {
		inst.prober.Stop()
	}
	if inst.httpSrv != nil {
		inst.httpSrv.Close()
	}
	if inst.defaultServerLn != nil {
		inst.defaultServerLn.Close()
	}
	if inst.defaultGRPCLn != nil {
		inst.defaultGRPCLn.Close()
	}
	if inst.grpcSrv != nil && !reloading {
		inst.grpcSrv.Stop()
	}
	shutdownTracing(inst.tracingShutdown)
}

func retireGRPCServer(srv *grpc.Server) {
	if srv == nil {
		return
	}
	go func() {
		timer := time.AfterFunc(5*time.Second, srv.Stop)
		defer timer.Stop()
		srv.GracefulStop()
	}()
}

// ReloadConfig rereads the original configuration source and replaces all
// configured components without restarting the process. Runtime state and
// counters are reset. If replacement fails, the previous configuration is
// reinitialized. Syntax errors do not interrupt the running instance.
func ReloadConfig(ctx context.Context) error {
	return reloadConfig(ctx, nil)
}

func reloadConfig(ctx context.Context, caller *prober.Prober) error {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if cloudProber.done || cloudProber.prober == nil || cloudProber.lifetimeCtx == nil {
		return status.Error(codes.FailedPrecondition, "cloudprober is not running")
	}
	if caller != nil && caller != cloudProber.prober {
		return status.Error(codes.Aborted, "this configuration has already been replaced; reconnect and retry")
	}
	if cloudProber.lifetimeCtx.Err() != nil {
		return status.Error(codes.FailedPrecondition, "cloudprober is shutting down")
	}
	if cloudProber.customGRPC {
		return status.Error(codes.FailedPrecondition, "reload requires a Cloudprober-managed gRPC server; an injected server cannot be recreated with its original options")
	}

	old := cloudProber.instance
	cfg, err := old.configSource.GetConfig()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "cannot read configuration: %v", err)
	}
	if cfg == nil {
		return status.Error(codes.InvalidArgument, "configuration is nil")
	}
	if err := proto.CheckInitialized(cfg); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid configuration: %v", err)
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}

	// Once teardown begins, finish replacement (or rollback) even if the caller
	// disconnects. Otherwise cancellation could leave the process without probes.
	old.stop(true)
	// Include API-added probes and omit API-removed probes if rollback is
	// needed. The source snapshot itself remains unchanged for config readers.
	rollbackConfig := proto.Clone(old.config).(*configpb.ProberConfig)
	activeProbes, _, _ := old.prober.SnapshotInfo()
	names := make([]string, 0, len(activeProbes))
	for name := range activeProbes {
		names = append(names, name)
	}
	sort.Strings(names)
	rollbackConfig.Probe = nil
	for _, name := range names {
		rollbackConfig.Probe = append(rollbackConfig.Probe, proto.Clone(activeProbes[name].ProbeDef).(*probes_configpb.ProbeDef))
	}
	// Start draining only after replacement or rollback finishes, leaving time
	// to deliver this RPC's response. Bound idle reflection streams as well.
	defer retireGRPCServer(old.grpcSrv)
	state.ResetRuntime()
	targets.ResetRuntime()
	alerting.ResetStatus()
	inst, applyErr := newInstance(old.configSource, withSystemProbe(cfg), true)
	if applyErr != nil {
		state.ResetRuntime()
		targets.ResetRuntime()
		inst, err = newInstance(old.configSource, rollbackConfig, true)
		if err != nil {
			cloudProber.Lock()
			cloudProber.instance = instance{
				configSource: old.configSource, config: old.config,
				rawConfig: old.rawConfig, parsedConfig: old.parsedConfig,
				lifetimeCtx: old.lifetimeCtx, cancelLifetime: old.cancelLifetime,
				stopWatch: old.stopWatch,
			}
			cloudProber.Unlock()
			return status.Errorf(codes.Internal, "reload failed: %v; restoring previous configuration also failed: %v", applyErr, err)
		}
		inst.rawConfig, inst.parsedConfig = old.rawConfig, old.parsedConfig
		inst.config = old.config
	}
	inst.lifetimeCtx, inst.cancelLifetime = old.lifetimeCtx, old.cancelLifetime
	inst.stopWatch = old.stopWatch
	cloudProber.Lock()
	inst.start()
	cloudProber.instance = inst
	cloudProber.Unlock()
	if applyErr != nil {
		return status.Errorf(codes.InvalidArgument, "reload failed; previous configuration restored: %v", applyErr)
	}
	return nil
}

// shutdownTracing flushes any buffered trace spans and shuts tracing down. The
// flush is bounded by tracingShutdownTimeout, so that an unreachable collector,
// which the exporter retries with a backoff, can't hold up the shutdown.
func shutdownTracing(shutdownFunc tracing.ShutdownFunc) {
	if shutdownFunc == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), tracingShutdownTimeout)
	defer cancel()
	if err := shutdownFunc(ctx); err != nil {
		slog.Warn("Error shutting down tracing", "err", err)
	}
}

// Shutdown stops the prober and the default servers, flushes any buffered
// trace spans, and releases the resources acquired by Init. It's safe to call
// more than once.
//
// It's one-way: cloudprober cannot be initialized again afterwards. Callers
// that use Start don't need to call Shutdown -- canceling the context passed
// to Start runs it for them -- it's meant for the paths that exit without ever
// starting the prober, e.g. RunOnce, which would otherwise leave the default
// servers' listeners open and buffered spans unexported.
//
// Shutdown waits for probe and server workers and closes their resources.
// It also stops the default gRPC server, including one that an embedder
// installed through state.SetDefaultGRPCServer.
func Shutdown() {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	cloudProber.Lock()
	if cloudProber.done {
		cloudProber.Unlock()
		return
	}
	cloudProber.done = true
	inst := cloudProber.instance
	cloudProber.instance = instance{}
	cloudProber.Unlock()

	if inst.stopWatch != nil {
		inst.stopWatch()
	}
	if inst.cancelLifetime != nil {
		inst.cancelLifetime()
	}
	// Keep compatibility for callers that supplied a gRPC server before Init.
	if inst.grpcSrv == nil {
		inst.grpcSrv = state.DefaultGRPCServer()
	}
	inst.stop(false)
	state.ResetRuntime()
	targets.ResetRuntime()
	alerting.ResetStatus()
}

// GetConfig returns the prober config.
func GetConfig() *configpb.ProberConfig {
	cloudProber.RLock()
	defer cloudProber.RUnlock()
	if cloudProber.config == nil {
		return nil
	}
	return proto.Clone(cloudProber.config).(*configpb.ProberConfig)
}

// GetRawConfig returns the prober config in text proto format.
func GetRawConfig() string {
	cloudProber.RLock()
	defer cloudProber.RUnlock()
	if cloudProber.config == nil && cloudProber.configSource == nil {
		return ""
	}
	if cloudProber.prober == nil && cloudProber.configSource != nil && cloudProber.lifetimeCtx == nil {
		return cloudProber.configSource.RawConfig()
	}
	return cloudProber.rawConfig
}

// GetParsedConfig returns the parsed prober config.
func GetParsedConfig() string {
	cloudProber.RLock()
	defer cloudProber.RUnlock()
	if cloudProber.config == nil && cloudProber.configSource == nil {
		return ""
	}
	if cloudProber.prober == nil && cloudProber.configSource != nil && cloudProber.lifetimeCtx == nil {
		return cloudProber.configSource.ParsedConfig()
	}
	return cloudProber.parsedConfig
}

// GetInfo returns information on all the probes, servers and surfacers.
func GetInfo() (map[string]*probes.ProbeInfo, []*surfacers.SurfacerInfo, []*servers.ServerInfo) {
	cloudProber.RLock()
	defer cloudProber.RUnlock()
	if cloudProber.prober == nil {
		return nil, nil, nil
	}
	return cloudProber.prober.SnapshotInfo()
}

func GetProber() *prober.Prober {
	cloudProber.RLock()
	defer cloudProber.RUnlock()
	return cloudProber.prober
}
