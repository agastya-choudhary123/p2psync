FROM rust:latest
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release && \
    cp target/release/p2psync /usr/local/bin/ && \
    mkdir -p /sync
WORKDIR /sync
ENTRYPOINT ["/usr/local/bin/p2psync"]
