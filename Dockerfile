FROM rust:1.86-bookworm AS rust

FROM rust AS builder
RUN apt-get update && apt-get -y --no-install-recommends install \
    protobuf-compiler \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ADD Cargo.toml .
ADD Cargo.lock .
ADD build.rs .
ADD proto proto
ADD src src
RUN cargo build --release


FROM rust AS proxy
WORKDIR /app
COPY --from=builder /app/target/release/sqd-yellowstone-geyser-proxy .
ENTRYPOINT ["/app/sqd-yellowstone-geyser-proxy"]