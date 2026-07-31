FROM rust:latest as builder
WORKDIR /build
COPY . .
RUN cargo build --release
RUN strip target/release/p2psync

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/p2psync /usr/local/bin/p2psync
RUN mkdir -p /sync
WORKDIR /sync
ENTRYPOINT ["p2psync", "."]
CMD ["-l", "0.0.0.0:7901", "--insecure"]
