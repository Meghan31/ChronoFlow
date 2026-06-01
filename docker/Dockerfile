# ── Stage 1: Build gRPC + Protobuf from source (heavily cached) ─────────────
FROM ubuntu:22.04 AS grpc-builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    pkg-config \
    ca-certificates \
    libssl-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Shallow-clone gRPC v1.54 with all submodules needed for build
RUN git clone --recurse-submodules -b v1.54.3 --depth 1 --shallow-submodules \
      https://github.com/grpc/grpc /tmp/grpc

RUN cmake -S /tmp/grpc -B /tmp/grpc/cmake/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/opt/grpc \
      -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DgRPC_BUILD_CSHARP_EXT=OFF \
      -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF \
      -DgRPC_ABSL_PROVIDER=module \
      -DgRPC_CARES_PROVIDER=module \
      -DgRPC_PROTOBUF_PROVIDER=module \
      -DgRPC_RE2_PROVIDER=module \
      -DgRPC_SSL_PROVIDER=package \
      -DgRPC_ZLIB_PROVIDER=package \
    && cmake --build /tmp/grpc/cmake/build --parallel $(nproc) \
    && cmake --install /tmp/grpc/cmake/build \
    && rm -rf /tmp/grpc

# ── Stage 2: Build ChronoFlow binaries ───────────────────────────────────────
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    libsqlite3-dev \
    libspdlog-dev \
    libgtest-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=grpc-builder /opt/grpc /opt/grpc

WORKDIR /app
COPY . .

RUN cmake -B build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_PREFIX_PATH="/opt/grpc;/usr" \
    && cmake --build build --parallel $(nproc)

# ── Stage 3: Scheduler runtime ───────────────────────────────────────────────
FROM ubuntu:22.04 AS scheduler

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    zlib1g \
    libsqlite3-0 \
    libspdlog1 \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=grpc-builder /opt/grpc/lib /opt/grpc/lib
COPY --from=builder /app/build/scheduler_service ./scheduler_service

ENV LD_LIBRARY_PATH=/opt/grpc/lib
EXPOSE 50051
CMD ["./scheduler_service"]

# ── Stage 4: Worker runtime ──────────────────────────────────────────────────
FROM ubuntu:22.04 AS worker

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    zlib1g \
    libsqlite3-0 \
    libspdlog1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=grpc-builder /opt/grpc/lib /opt/grpc/lib
COPY --from=builder /app/build/worker_service ./worker_service

ENV LD_LIBRARY_PATH=/opt/grpc/lib
EXPOSE 50052
ENTRYPOINT ["./worker_service"]
CMD ["worker-1", "50052", "scheduler:50051"]
