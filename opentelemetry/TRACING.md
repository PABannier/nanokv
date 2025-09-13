# Distributed Tracing with OpenTelemetry

NanoKV supports distributed tracing using OpenTelemetry with OTLP export to Jaeger, Tempo, or other compatible backends.

## Quick Start

### 1. Start Jaeger (Docker)

```bash
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 14268:14268 \
  -p 14250:14250 \
  -p 4317:4317 \
  -p 4318:4318 \
  jaegertracing/all-in-one:latest
```

### 2. Configure Environment

Copy the example environment file:

```bash
cp tracing.env.example .env
```

Edit `.env` to configure the OTLP endpoint:

```bash
OTEL_TRACES_EXPORTER=otlp
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
RUST_LOG=nanokv=info,coord=info,volume=info
```

### 3. Run with Tracing

Start your services with the environment variables loaded:

```bash
# Load environment
source .env

# Start coordinator
cargo run --bin coord -- serve --listen 0.0.0.0:8080

# Start volume servers
cargo run --bin volume -- --coordinator-url http://localhost:8080 --public-url http://localhost:8081
```

### 4. View Traces

Open Jaeger UI at http://localhost:16686 and look for traces from the `coord` and `volume` services.

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OTEL_TRACES_EXPORTER` | Set to `otlp` to enable OTLP export | stdout |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint URL | `http://localhost:4317` |
| `OTEL_SERVICE_NAME` | Override service name | Binary name (`coord`/`volume`) |
| `RUST_LOG` | Log level configuration | `info` |

### Backends

#### Jaeger
- gRPC: `http://localhost:4317`
- HTTP: `http://localhost:4318`
- UI: http://localhost:16686

#### Grafana Tempo
- gRPC: `http://localhost:4317`
- HTTP: `http://localhost:4318`

#### Other OTLP-compatible backends
Configure the endpoint URL accordingly.

## Trace Structure

### PUT Operation Trace
A complete PUT operation creates the following trace structure:

```
coord.put (root span)
├── sanity_check
├── choose_placement
├── prepare
│   ├── prepare_replica (replica-1)
│   ├── prepare_replica (replica-2)
│   └── prepare_replica (replica-3)
├── write_to_head
├── pull
│   ├── pull_replica (replica-2 ← replica-1)
│   └── pull_replica (replica-3 ← replica-1)
├── commit
│   ├── commit_replica (replica-1)
│   ├── commit_replica (replica-2)
│   └── commit_replica (replica-3)
└── meta.write
```

### Volume Operations
Each volume operation creates instrumented spans:

- `volume.prepare` - Prepare temporary file
- `volume.write` - Write data to temporary file  
- `volume.pull` - Pull data from head node
- `volume.commit` - Commit temporary file to final location

### Trace Context Propagation
- HTTP requests between coordinator and volumes automatically propagate trace context
- Per-replica operations run in parallel with individual child spans
- All spans include relevant metadata (replica IDs, upload IDs, keys)

## Development

### Stdout Export (Development)
For development without external tracing infrastructure:

```bash
# Disable OTLP, use stdout
unset OTEL_TRACES_EXPORTER
# or
OTEL_TRACES_EXPORTER=stdout

RUST_LOG=nanokv=debug cargo run --bin coord -- serve
```

### Custom Instrumentation
Add custom spans in your code:

```rust
use tracing::{info_span, Instrument};

let span = info_span!("custom_operation", key = %key);
async_operation().instrument(span).await
```

## Troubleshooting

### No traces appearing
1. Check OTLP endpoint is reachable
2. Verify `OTEL_TRACES_EXPORTER=otlp` is set
3. Check service logs for OpenTelemetry errors
4. Ensure Jaeger/Tempo is running and accessible

### Performance Impact
- Tracing has minimal overhead when properly configured
- Use sampling for high-throughput production workloads
- Consider using `OTEL_TRACES_SAMPLER=traceidratio` with `OTEL_TRACES_SAMPLER_ARG=0.1` for 10% sampling

### Network Issues
- OTLP uses gRPC by default (port 4317)
- HTTP endpoint available on port 4318
- Configure firewall/network policies accordingly
