FROM python:3.12-alpine

ARG MEDIA_PIPELINE_VERSION=0.2.1
ARG MEDIA_PIPELINE_REVISION=unknown

LABEL org.opencontainers.image.title="Media Pipeline" \
      org.opencontainers.image.version="${MEDIA_PIPELINE_VERSION}" \
      org.opencontainers.image.revision="${MEDIA_PIPELINE_REVISION}"

RUN apk add --no-cache 7zip libarchive-tools

WORKDIR /app
COPY app/ /app/

ENV PYTHONUNBUFFERED=1
ENV MEDIA_PIPELINE_VERSION=${MEDIA_PIPELINE_VERSION}
ENV MEDIA_PIPELINE_REVISION=${MEDIA_PIPELINE_REVISION}
ENTRYPOINT ["python", "-m", "pipeline.bot"]
