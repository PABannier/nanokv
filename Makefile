# Increase thresholds as we do not want reports to fail due to latency
# as it would stop the benchmarks. We simply want to collect the data.
PUT_P95_THRESHOLD=100000
GET_P95_THRESHOLD=100000

# Object sizes in bytes
SIZE_1MB=1048576
SIZE_4MB=4194304
SIZE_16MB=16777216
SIZE_64MB=67108864

single-volume:
	cd bench && ./run_benchmark.sh 1 1 5600 5601 128 "60s" $(SIZE_64MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_single_volume.json"

perf-smoke:
	cd bench && ./run_benchmark.sh 3 2 5600 5601 16 "60s" $(SIZE_1MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_smoke.json"

bench-concurrency:
	cd bench && ./run_benchmark.sh 3 2 5600 5601  16 "60s" $(SIZE_1MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_put_16_vus.json"
	cd bench && ./run_benchmark.sh 3 2 5600 5601  64 "60s" $(SIZE_1MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_put_64_vus.json"
	cd bench && ./run_benchmark.sh 3 2 5600 5601 128 "60s" $(SIZE_1MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_put_128_vus.json"

bench-object-size:
	cd bench && ./run_benchmark.sh 3 2 5600 5601 64 "60s" $(SIZE_1MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_latency_1mb.json"
	cd bench && ./run_benchmark.sh 3 2 5600 5601 64 "60s" $(SIZE_4MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_latency_4mb.json"
	cd bench && ./run_benchmark.sh 3 2 5600 5601 64 "60s" $(SIZE_16MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_latency_16mb.json"
	cd bench && ./run_benchmark.sh 3 2 5600 5601 64 "60s" $(SIZE_64MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_latency_64mb.json"

debug:
	cd bench && ./run_benchmark.sh 3 2 5600 5601 64 "60s" $(SIZE_64MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_latency_64mb.json"

bench-replication:
	cd bench && ./run_benchmark.sh 5 1 5600 5601 64 "60s" $(SIZE_1MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_replication_1.json"
	cd bench && ./run_benchmark.sh 5 2 5600 5601 64 "60s" $(SIZE_1MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_replication_2.json"
	cd bench && ./run_benchmark.sh 5 3 5600 5601 64 "60s" $(SIZE_1MB) $(PUT_P95_THRESHOLD) $(GET_P95_THRESHOLD) "./perf/perf_replication_3.json"

bench-suite: bench-concurrency bench-object-size bench-replication

pre-commit-checks:
	cd src && cargo fmt --all
	cd src && cargo clippy --all-targets -- -D warnings

test:
	cd src && cargo test --all --all-features --locked
