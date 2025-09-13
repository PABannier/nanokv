#!/bin/bash

set -euo pipefail

mkdir -p perf

# Increase thresholds as we do not want reports to fail due to latency
# as it would stop the benchmarks. We simply want to collect the data.
PUT_P95_THRESHOLD=100000
GET_P95_THRESHOLD=100000

# PUT throughput vs.concurrency
./run_benchmark.sh 3 2 5600 5601  16 "60s" $((1<<20)) $PUT_P95_THRESHOLD $GET_P95_THRESHOLD "./perf/perf_put_16_vus.json"
./run_benchmark.sh 3 2 5600 5601  64 "60s" $((1<<20)) $PUT_P95_THRESHOLD $GET_P95_THRESHOLD "./perf/perf_put_64_vus.json"
./run_benchmark.sh 3 2 5600 5601 128 "60s" $((1<<20)) $PUT_P95_THRESHOLD $GET_P95_THRESHOLD "./perf/perf_put_128_vus.json"

# Latency vs. object size (VUs=64, N=2)
./run_benchmark.sh 3 2 5600 5601 64 "60s" $((1<<20)) $PUT_P95_THRESHOLD $GET_P95_THRESHOLD "./perf/perf_latency_1mb.json"
./run_benchmark.sh 3 2 5600 5601 64 "60s" $((1<<22)) $PUT_P95_THRESHOLD $GET_P95_THRESHOLD "./perf/perf_latency_4mb.json"
./run_benchmark.sh 3 2 5600 5601 64 "60s" $((1<<24)) $PUT_P95_THRESHOLD $GET_P95_THRESHOLD "./perf/perf_latency_16mb.json"
./run_benchmark.sh 3 2 5600 5601 64 "60s" $((1<<26)) $PUT_P95_THRESHOLD $GET_P95_THRESHOLD "./perf/perf_latency_64mb.json"

# Replication factor impact
./run_benchmark.sh 5 1 5600 5601 64 "60s" $((1<<20)) $PUT_P95_THRESHOLD $GET_P95_THRESHOLD "./perf/perf_replication_1.json"
./run_benchmark.sh 5 2 5600 5601 64 "60s" $((1<<20)) $PUT_P95_THRESHOLD $GET_P95_THRESHOLD "./perf/perf_replication_2.json"
./run_benchmark.sh 5 3 5600 5601 64 "60s" $((1<<20)) $PUT_P95_THRESHOLD $GET_P95_THRESHOLD "./perf/perf_replication_3.json"

# TODO: Background ops interference (verify, gc)
