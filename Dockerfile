FROM rust:1.85-alpine AS builder
RUN apk add --no-cache musl-dev cmake perl make
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM alpine:3.21
RUN addgroup -S flux && adduser -S -G flux flux
COPY --from=builder /src/target/release/flux /usr/local/bin/flux
USER flux
ENTRYPOINT ["/usr/local/bin/flux"]
