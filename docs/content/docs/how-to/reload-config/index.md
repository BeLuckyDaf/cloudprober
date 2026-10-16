---
menu:
  docs:
    parent: 'how-to'
    weight: 25
title: 'Reload Configuration over gRPC'
date: 2026-10-16
---

Cloudprober's `cloudprober.Cloudprober/ReloadConfig` RPC rereads the original
configuration source and applies the complete configuration without restarting
its process. It supports probe definitions, shared targets and discovery,
metric exporters, built-in servers, HTTP and gRPC listener addresses, gRPC TLS,
and tracing settings.

## Enable the management service

Configure a gRPC port before starting Cloudprober:

```textproto
host: "127.0.0.1"
grpc_port: 9314
```

Alternatively, set `CLOUDPROBER_GRPC_PORT`. The reload method uses the same
listener and TLS settings as the existing probe management RPCs. Bind the
management listener to an appropriate interface and restrict access to it.

After editing the configuration file, invoke:

```sh
grpcurl -plaintext -d '{}' 127.0.0.1:9314 \
  cloudprober.Cloudprober/ReloadConfig
```

For a TLS-enabled listener, use the corresponding `grpcurl` TLS options instead
of `-plaintext`. Server reflection is enabled on the default management listener,
so a separate descriptor file is not required.

A successful response is `{}`. If the gRPC address or TLS settings changed,
connect subsequent requests to the new endpoint with its new credentials. The
reload response is sent over the original connection before that server drains.
Disabling the gRPC listener also disables further remote management requests.

Go clients use the generated client:

```go
_, err := client.ReloadConfig(ctx, &proberpb.ReloadConfigRequest{})
```

Here `proberpb` is `github.com/cloudprober/cloudprober/prober/proto` and `client`
is a `proberpb.CloudproberClient`. Applications embedding Cloudprober can call
`cloudprober.ReloadConfig(ctx)` directly after `cloudprober.Start`.

## Reload behavior

The RPC takes no file path or configuration payload. It rereads the source passed
to `InitWithConfigSource`, or the default source used by `Init`. Templates,
includes, environment substitution, YAML/JSON/Jsonnet, metadata configuration,
and a separately configured surfacers file go through the same loading path as
startup. Command-line flags retain their original values.

Reload stops the current components and initializes replacements. There is a
brief interruption in probing and serving, and in-memory counters, probe status
history, and exporter state reset. This is a full runtime replacement, including
unchanged probes. Unsaved changes made through `AddProbe` or `RemoveProbe` are
superseded by the configuration source. Retained application logs survive reload. Existing file-exporter output is
appended during reload and rollback.

A read or parse error returns `InvalidArgument` without stopping the running
instance. If component initialization fails, Cloudprober attempts to reinitialize
the previous configuration and reports the failure. Rollback resets runtime state
as well. If rollback also fails, the RPC returns `Internal` with both errors;
the process remains alive but monitoring requires operator recovery. Rollback
cannot restore external files or services that have changed independently.

Reload requests are serialized. Calls arriving at a retired management server
are rejected; reconnect before retrying. Cancellation before teardown aborts
reload. Once teardown has begun, replacement or rollback completes even if the
calling client disconnects. A client timeout therefore does not establish
whether the new configuration was applied; check the active configuration.

`FailedPrecondition` is returned before startup, during shutdown, or when an
embedding application injected its own gRPC server with
`state.SetDefaultGRPCServer`. Cloudprober cannot recreate the custom server's
interceptors, credentials, and other options. Custom probes, servers, surfacers, and target
extensions must honor cancellation and release owned resources; optional
`Close() error` methods are called during teardown.
