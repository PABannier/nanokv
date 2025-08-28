#!/bin/bash

cargo build


echo "Running fast size-only checks"
cargo run -- \
    --data ./data --index ./data/index \
    verify

echo "Running deep checks"
cargo run -- \
    --data ./data --index ./data/index \
    --deep-verify \
    verify
