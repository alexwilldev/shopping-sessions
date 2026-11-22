# Shopping Sessions v3: native Rust binary in a slim runtime image.
# The scorers are linux/amd64 (Intel Xeon, Cascade Lake or newer, AVX-512). No platform is
# pinned, so `make build` with PLATFORM=linux/arm64 also works for fast local loops.
FROM rust:1-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
# Build dependencies first so code edits rebuild quickly.
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src
COPY rust/src ./src
# x86-64-v3 (AVX2) on amd64: every scoring machine has it (they all have AVX-512).
RUN if [ "$TARGETARCH" = "amd64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi; \
    touch src/main.rs && cargo build --release && cp target/release/shopsess /shopsess

FROM debian:bookworm-slim
COPY --from=build /shopsess /app/shopsess
# Transparent huge pages for the allocator's arenas (the scorer runs THP in madvise mode).
ENV MIMALLOC_ALLOW_LARGE_OS_PAGES=1 MIMALLOC_LARGE_OS_PAGES=1
# The scorer runs as uid 1000 with a read-only root; the binary writes only to $OUTPUT_DIR.
CMD ["/app/shopsess"]

# A/B variant t16 (speed only; results identical)
ENV NCPU=16
