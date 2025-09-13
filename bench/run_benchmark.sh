#!/bin/bash

set -euo pipefail

# Default configuration
DEFAULT_VOLUMES=3
DEFAULT_REPLICAS=3
DEFAULT_COORD_PORT=5600
DEFAULT_VOLUME_START_PORT=5601
DEFAULT_K6_VUS=16
DEFAULT_K6_DURATION="45s"
DEFAULT_K6_SIZE=$((1<<20))
DEFAULT_K6_PUT_P95_THRESHOLD=1500
DEFAULT_K6_GET_P95_THRESHOLD=100
DEFAULT_K6_SUMMARY_EXPORT="perf.json"

# Parse command line arguments
VOLUMES=${1:-$DEFAULT_VOLUMES}
REPLICAS=${2:-$DEFAULT_REPLICAS}
COORD_PORT=${3:-$DEFAULT_COORD_PORT}
VOLUME_START_PORT=${4:-$DEFAULT_VOLUME_START_PORT}
K6_VUS=${5:-$DEFAULT_K6_VUS}
K6_DURATION=${6:-$DEFAULT_K6_DURATION}
K6_SIZE=${7:-$DEFAULT_K6_SIZE}
K6_PUT_P95_THRESHOLD=${8:-$DEFAULT_K6_PUT_P95_THRESHOLD}
K6_GET_P95_THRESHOLD=${9:-$DEFAULT_K6_GET_P95_THRESHOLD}
K6_SUMMARY_EXPORT=${10:-$DEFAULT_K6_SUMMARY_EXPORT}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Usage function
usage() {
    cat << EOF
Usage: $0 [volumes] [replicas] [coord_port] [volume_start_port] [k6_vus] [k6_duration] [k6_size] [k6_put_p95_threshold] [k6_get_p95_threshold] [k6_summary_export]

Arguments:
  volumes           Number of volume servers to launch (default: $DEFAULT_VOLUMES)
  replicas          Number of replicas for data (default: $DEFAULT_REPLICAS)
  coord_port        Coordinator port (default: $DEFAULT_COORD_PORT)
  volume_start_port Starting port for volume servers (default: $DEFAULT_VOLUME_START_PORT)
  k6_vus           K6 virtual users (default: $DEFAULT_K6_VUS)
  k6_duration      K6 test duration (default: $DEFAULT_K6_DURATION)
  k6_size          Object size in bytes (default: $DEFAULT_K6_SIZE)
  k6_put_p95_threshold P95 threshold for PUT requests (default: $DEFAULT_K6_PUT_P95_THRESHOLD)
  k6_get_p95_threshold P95 threshold for GET requests (default: $DEFAULT_K6_GET_P95_THRESHOLD)
  k6_summary_export Path to k6 summary export file (default: $DEFAULT_K6_SUMMARY_EXPORT)

Examples:
  $0                                                      # Use all defaults
  $0 5                                                    # 5 volumes, other defaults
  $0 5 3 3000 3001 32 "60s" 2097152 1500 100 "perf.json"  # Full configuration

Environment Variables:
  K6_BINARY        Path to k6 binary (default: k6)
EOF
}

# Check if help is requested
if [[ "${1:-}" == "-h" ]] || [[ "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

# Validate arguments
if [[ $VOLUMES -lt 1 ]]; then
    log_error "Number of volumes must be at least 1"
    exit 1
fi

if [[ $REPLICAS -lt 1 ]] || [[ $REPLICAS -gt $VOLUMES ]]; then
    log_error "Number of replicas must be between 1 and $VOLUMES"
    exit 1
fi

# Array to store background process PIDs
PIDS=()
TEMP_DIRS=()

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Cleanup function
cleanup() {
    log_info "Cleaning up processes and temporary directories..."

    # Kill all background processes
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            log_info "Stopping process $pid"
            kill "$pid" 2>/dev/null || true
        fi
    done

    # Wait a bit for graceful shutdown
    sleep 2

    # Force kill if still running
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            log_warn "Force killing process $pid"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done

    # Clean up temporary directories
    for temp_dir in "${TEMP_DIRS[@]}"; do
        if [[ -d "$temp_dir" ]]; then
            log_info "Removing temporary directory: $temp_dir"
            rm -rf "$temp_dir"
        fi
    done

    log_success "Cleanup completed"
}

# Set up signal handlers
trap cleanup EXIT INT TERM

# Check dependencies
check_dependencies() {
    log_info "Checking dependencies..."

    # Check if cargo is available
    if ! command -v cargo &> /dev/null; then
        log_error "cargo is not installed or not in PATH"
        exit 1
    fi

    # Check if k6 is available
    K6_BINARY=${K6_BINARY:-k6}
    if ! command -v "$K6_BINARY" &> /dev/null; then
        log_error "k6 is not installed or not in PATH. Install from https://k6.io/docs/getting-started/installation/"
        exit 1
    fi

    
    # Check if benchmark file exists
    if [[ ! -f "$SCRIPT_DIR/scenarios/bench_coord_put_get.js" ]]; then
        log_error "Benchmark file $SCRIPT_DIR/scenarios/bench_coord_put_get.js not found"
        exit 1
    fi

    log_success "All dependencies are available"
}

# Build binaries
build_binaries() {
    log_info "Building coordinator and volume binaries..."

    cd "$PROJECT_ROOT/src"
    if ! cargo build --release; then
        log_error "Failed to build binaries"
        exit 1
    fi
    cd "$SCRIPT_DIR"

    # Verify binaries exist
    if [[ ! -f "$PROJECT_ROOT/src/target/release/coord" ]]; then
        log_error "Coordinator binary not found at $PROJECT_ROOT/src/target/release/coord"
        exit 1
    fi

    if [[ ! -f "$PROJECT_ROOT/src/target/release/volume" ]]; then
        log_error "Volume binary not found at $PROJECT_ROOT/src/target/release/volume"
        exit 1
    fi

    log_success "Binaries built successfully"
}

# Create temporary directories
create_temp_dirs() {
    log_info "Creating temporary directories..."

    # Create coordinator data directory
    COORD_DATA_DIR=$(mktemp -d -t nanokv-coord-XXXXXX)
    TEMP_DIRS+=("$COORD_DATA_DIR")

    # Create volume data directories
    VOLUME_DATA_DIRS=()
    for ((i=1; i<=VOLUMES; i++)); do
        vol_dir=$(mktemp -d -t nanokv-vol$i-XXXXXX)
        VOLUME_DATA_DIRS+=("$vol_dir")
        TEMP_DIRS+=("$vol_dir")
    done

    log_success "Temporary directories created"
}

# Start coordinator
start_coordinator() {
    log_info "Starting coordinator on port $COORD_PORT..."

    # Build volume URLs for coordinator
    VOLUME_URLS=""
    for ((i=1; i<=VOLUMES; i++)); do
        port=$((VOLUME_START_PORT + i - 1))
        if [[ $i -eq 1 ]]; then
            VOLUME_URLS="http://127.0.0.1:$port"
        else
            VOLUME_URLS="$VOLUME_URLS,http://127.0.0.1:$port"
        fi
    done

    # Start coordinator in background
    "$PROJECT_ROOT/src/target/release/coord" serve \
        --data "$COORD_DATA_DIR" \
        --index "$COORD_DATA_DIR/index" \
        --listen "127.0.0.1:$COORD_PORT" \
        --n-replicas "$REPLICAS" \
        --max-inflight 8 \
        --hb-alive-secs 5 \
        --hb-down-secs 20 \
        --node-status-sweep-secs 1 \
        --pending-grace-secs 600 &

    COORD_PID=$!
    PIDS+=("$COORD_PID")

    log_success "Coordinator started (PID: $COORD_PID)"
}

# Start volume servers
start_volumes() {
    log_info "Starting $VOLUMES volume servers..."

    for ((i=1; i<=VOLUMES; i++)); do
        port=$((VOLUME_START_PORT + i - 1))
        vol_data_dir="${VOLUME_DATA_DIRS[$((i-1))]}"

        log_info "Starting volume server $i on port $port..."

        "$PROJECT_ROOT/src/target/release/volume" \
            --data "$vol_data_dir" \
            --coordinator-url "http://127.0.0.1:$COORD_PORT" \
            --node-id "vol$i" \
            --public-url "http://127.0.0.1:$port" \
            --internal-url "http://127.0.0.1:$port" \
            --subvols 1 \
            --heartbeat-interval-secs 1 \
            --http-timeout-secs 10 &

        vol_pid=$!
        PIDS+=("$vol_pid")

        log_success "Volume server $i started (PID: $vol_pid)"
    done
}

# Wait for services to be ready
wait_for_services() {
    log_info "Waiting for services to be ready..."

    # Wait for coordinator
    local coord_ready=false
    for ((i=1; i<=30; i++)); do
        if curl -s "http://127.0.0.1:$COORD_PORT/admin/nodes" > /dev/null 2>&1; then
            coord_ready=true
            break
        fi
        sleep 1
    done

    if [[ "$coord_ready" != true ]]; then
        log_error "Coordinator failed to start within 30 seconds"
        exit 1
    fi

    log_success "Coordinator is ready"

    # Wait for all volumes to join
    log_info "Waiting for volume servers to join cluster..."
    for ((i=1; i<=60; i++)); do
        node_count=$(curl -s "http://127.0.0.1:$COORD_PORT/admin/nodes" | jq -r '. | length' 2>/dev/null || echo "0")
        if [[ "$node_count" -eq "$VOLUMES" ]]; then
            log_success "All $VOLUMES volume servers have joined the cluster"
            break
        fi

        if [[ $((i % 10)) -eq 0 ]]; then
            log_info "Waiting for volumes to join... ($node_count/$VOLUMES joined)"
        fi
        sleep 1
    done

    # Final check
    node_count=$(curl -s "http://127.0.0.1:$COORD_PORT/admin/nodes" | jq -r '. | length' 2>/dev/null || echo "0")
    if [[ "$node_count" -ne "$VOLUMES" ]]; then
        log_error "Only $node_count/$VOLUMES volume servers joined the cluster"
        exit 1
    fi

    log_success "Cluster is ready with $VOLUMES volume servers"
}

# Run k6 benchmark
run_benchmark() {
    log_info "Running k6 benchmark..."
    log_info "Configuration: VUs=$K6_VUS, Duration=$K6_DURATION, Size=$K6_SIZE bytes"

    # Export environment variables for k6
    export BASE="http://127.0.0.1:$COORD_PORT"
    export VUS="$K6_VUS"
    export DUR="$K6_DURATION"
    export SIZE="$K6_SIZE"
    export PUT_P95_THRESHOLD="$K6_PUT_P95_THRESHOLD"
    export GET_P95_THRESHOLD="$K6_GET_P95_THRESHOLD"

    # Export environment variables for tracing
    export OTEL_TRACES_EXPORTER=otlp
    export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
    export RUST_LOG=nanokv=info,coord=info,volume=info,common=info

    # Run k6 benchmark
    if "$K6_BINARY" run "$SCRIPT_DIR/scenarios/bench_coord_put_get.js" --summary-export "$K6_SUMMARY_EXPORT"; then
        log_success "Benchmark completed successfully"
    else
        log_error "Benchmark failed"
        exit 1
    fi
}

# Main execution
main() {
    log_info "Starting nanokv benchmark with $VOLUMES volumes, $REPLICAS replicas"
    log_info "Coordinator port: $COORD_PORT, Volume ports: $VOLUME_START_PORT-$((VOLUME_START_PORT + VOLUMES - 1))"

    check_dependencies
    build_binaries
    create_temp_dirs
    start_coordinator

    # Give coordinator a moment to start
    sleep 2

    start_volumes
    wait_for_services

    # Show cluster status
    log_info "Cluster status:"
    curl -s "http://127.0.0.1:$COORD_PORT/admin/nodes" | jq -r '.[] | "\(.node_id): \(.status) (\(.public_url))"' || true

    run_benchmark

    log_success "Benchmark run completed!"
}

# Run main function
main "$@"