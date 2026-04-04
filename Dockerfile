# gpufl-agent — build image
#
# Publishes the fat JAR to ghcr.io/gpu-flight/gpufl-agent so it can be
# consumed by Dockerfile.monitor via COPY --from=ghcr.io/...
#
# The image contains only the JAR at /app/gpufl-agent.jar.
# Runtime config is injected at container start via GPUFL_* env vars —
# not via the bundled local.json, which is a dev-only fallback.

FROM eclipse-temurin:25-jdk AS builder
WORKDIR /app
COPY . .
RUN ./gradlew shadowJar --no-daemon -q

FROM scratch
COPY --from=builder /app/build/libs/*-all.jar /app/gpufl-agent.jar
