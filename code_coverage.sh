#!/bin/bash
(export RUSTFLAGS="-Cinstrument-coverage" &&
rm -f lcov.info &&
cargo clean &&
cargo test &&
grcov . --binary-path ./target/debug/ -s . -t lcov --branch --ignore-not-existing  --output-path ./lcov.info --ignore "*/ningwang/*" &&
rm -f default*
)
cargo build

