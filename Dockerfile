# syntax=docker/dockerfile:1.7
ARG ELIXIR_IMAGE=hexpm/elixir:1.20.4-erlang-29.1.2-alpine-3.24.2
ARG RUNTIME_IMAGE=alpine:3.24.2

FROM ${ELIXIR_IMAGE} AS build
# build-base and cmake compile the CRC32C NIF that brod (Kafka) needs.
RUN apk add --no-cache build-base cmake git
WORKDIR /app
ENV MIX_ENV=prod
RUN mix local.hex --force && mix local.rebar --force
COPY mix.exs mix.lock ./
RUN --mount=type=cache,target=/root/.hex/packages \
    mix deps.get --only prod
# Dependencies compile against the compile-time config only; runtime.exs is read at boot, so changing it must
# not recompile every dependency.
COPY config/config.exs config/prod.exs config/
RUN mix deps.compile
COPY lib lib
COPY rel rel
COPY config/runtime.exs config/
RUN mix release

FROM ${RUNTIME_IMAGE} AS runtime
ARG VERSION=0.1.0
ARG REVISION=unknown
LABEL org.opencontainers.image.title="discordy-gateway" \
      org.opencontainers.image.description="Discordy realtime gateway: WebSocket sessions, guild fan-out, voice allocation, E2EE and P2P relay" \
      org.opencontainers.image.source="https://github.com/discordy-voics/gateway" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="UNLICENSED"
RUN apk add --no-cache libstdc++ openssl ncurses-libs ca-certificates \
 && addgroup -S -g 10001 gateway \
 && adduser -S -u 10001 -G gateway gateway
WORKDIR /app
COPY --from=build --chown=gateway:gateway /app/_build/prod/rel/discordy_gateway ./
USER 10001:10001
ENV LANG=C.UTF-8 \
    PORT=4000
EXPOSE 4000
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:${PORT}/healthz || exit 1
ENTRYPOINT ["/app/bin/discordy_gateway"]
CMD ["start"]
