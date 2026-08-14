FROM rust:1-bookworm AS builder

WORKDIR /app
COPY . .
RUN cargo build --release --locked --bins

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/server /usr/local/bin/server
COPY --from=builder /app/target/release/trading-client /usr/local/bin/trading-client
COPY --from=builder /app/target/release/control-client /usr/local/bin/control-client
COPY --from=builder /app/target/release/replay /usr/local/bin/replay
COPY --from=builder /app/target/release/benchmark /usr/local/bin/benchmark
COPY configs ./configs
COPY fixtures/replay ./fixtures/replay

ENV RUST_LOG=info
CMD ["server", "--config", "configs/server.docker.yaml"]
