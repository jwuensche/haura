#! /bin/env bash

num_thread=$(echo "$(cat /proc/meminfo | head -n 1 | xargs | cut -d ' ' -f 2) / 1024 / 1024 / 2" | bc)
cargo test -- --test-threads "$num_thread" -Z unstable-options --format json > tests.out
failed=$(cat tests.out | tail -n 1 | jq '.failed')
passed=$(cat tests.out | tail -n 1 | jq '.passed')
exit $failed
