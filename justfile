fix:
    cargo fmt --all
    cargo clippy --all-targets -- -W unused_imports -W clippy::all

server:
    cargo run --bin server

sbe:
    scripts/generate_sbe.sh
