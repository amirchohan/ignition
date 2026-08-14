fix:
    cargo fmt --all
    cargo clippy --all-targets -- -W unused_imports -W clippy::all

server:
    cargo run --bin server

trading-client:
    cargo run --bin trading-client

control-client:
    cargo run --bin control-client

replay input_id:
    cargo run --bin replay -- --run {{input_id}} --config configs/replay.yaml

replay-compare input_id:
    scripts/replay_compare.sh {{input_id}} configs/replay.yaml

bench:
    cargo run --release --bin benchmark -- --orders 1000000 --warmup 50000 --workload mixed

bench-open-loop rate:
    cargo run --release --bin benchmark -- --orders 1000000 --warmup 50000 --workload mixed --schedule open-loop --target-rate {{rate}}

sbe:
    scripts/generate_sbe.sh
