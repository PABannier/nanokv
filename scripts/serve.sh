#!/bin/bash

# Sanity check: up-to-date
cargo build

cargo run -- \
    --data ./data --index ./data/index \
    --pending-grace-secs 600 \
    serve
