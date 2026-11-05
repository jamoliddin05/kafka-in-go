# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26.9

FROM golang:${GO_VERSION}-alpine AS base
WORKDIR /src
ENV CGO_ENABLED=0

# ---------- dev: what docker compose runs; source is bind-mounted over /src ----------
# Modules are not baked in: they live in the `gomod` compose volume, so changing
# go.mod never forces a rebuild of this image.
FROM base AS dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go install github.com/air-verse/air@v1.67.4
CMD ["sh", "-c", "exec air -c .air.toml --build.include_dir \"internal,tasks/${TASK}\""]

# ---------- build: compiles exactly one task into a static binary ----------
FROM base AS build
ARG TASK
COPY . .
# Cache mounts keep modules and build cache across builds, so a go.sum change
# downloads only the delta instead of invalidating a whole layer.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test -n "${TASK}" || (echo "pass --build-arg TASK=<dir under tasks/>" && exit 1) && \
    go build -trimpath -ldflags="-s -w" -o /out/app "./tasks/${TASK}"

# ---------- prod: no shell, no toolchain, no air; PID 1 is the app so it gets SIGTERM directly ----------
FROM gcr.io/distroless/static-debian13:nonroot AS prod
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
