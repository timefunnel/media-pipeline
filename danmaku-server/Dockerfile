FROM node:22-bookworm-slim

ARG DANMAKU_VERSION=local
ARG DANMAKU_REVISION=unknown
LABEL org.opencontainers.image.title="MediaStation Danmaku Server" \
      org.opencontainers.image.version="${DANMAKU_VERSION}" \
      org.opencontainers.image.revision="${DANMAKU_REVISION}"

RUN apt-get update && apt-get install -y --no-install-recommends python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app/danmaku-server
COPY danmaku-server/package.json danmaku-server/package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund --legacy-peer-deps && npm cache clean --force
COPY danmaku-server/bridge.mjs danmaku-server/youku.mjs danmaku-server/risk.mjs ./
COPY app/danmaku /app/danmaku
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1 DANMAKU_SERVER_HOST=0.0.0.0 \
    DANMAKU_BRIDGE=/app/danmaku-server/bridge.mjs \
    DANMAKU_VERSION=${DANMAKU_VERSION} DANMAKU_REVISION=${DANMAKU_REVISION}
EXPOSE 9322
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9322/healthz', timeout=3).read()" || exit 1
ENTRYPOINT ["python3", "-m", "danmaku.server"]
