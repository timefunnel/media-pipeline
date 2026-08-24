#!/bin/sh
set -eu

usage() {
    echo "usage: $0 <image@sha256:digest> <expected-40-char-revision> <expected-version>" >&2
    exit 2
}

[ "$#" -eq 3 ] || usage

image_ref=$1
expected_revision=$2
expected_version=$3

if ! printf '%s\n' "$image_ref" | grep -Eq '^.+@sha256:[0-9a-f]{64}$'; then
    echo "image reference must contain an immutable sha256 digest: $image_ref" >&2
    exit 2
fi

if ! printf '%s\n' "$expected_revision" | grep -Eq '^[0-9a-f]{40}$'; then
    echo "expected revision must be a full 40-character Git SHA" >&2
    exit 2
fi

if [ -z "$expected_version" ]; then
    echo "expected version must not be empty" >&2
    exit 2
fi

docker pull --platform linux/amd64 "$image_ref"

requested_digest=${image_ref##*@}
repo_digests=$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$image_ref")
actual_platform=$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image_ref")
actual_revision=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$image_ref")
actual_version=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$image_ref")
image_id=$(docker image inspect --format '{{.Id}}' "$image_ref")

if ! printf '%s\n' "$repo_digests" | grep -Fq "@$requested_digest"; then
    echo "pulled image does not report the requested repository digest: $requested_digest" >&2
    exit 1
fi

if [ "$actual_platform" != "linux/amd64" ]; then
    echo "image platform mismatch: expected linux/amd64, got $actual_platform" >&2
    exit 1
fi

if [ "$actual_revision" != "$expected_revision" ]; then
    echo "image revision mismatch: expected $expected_revision, got $actual_revision" >&2
    exit 1
fi

if [ "$actual_version" != "$expected_version" ]; then
    echo "image version mismatch: expected $expected_version, got $actual_version" >&2
    exit 1
fi

printf 'IMAGE_REF=%s\n' "$image_ref"
printf 'IMAGE_ID=%s\n' "$image_id"
printf 'IMAGE_PLATFORM=%s\n' "$actual_platform"
printf 'IMAGE_REVISION=%s\n' "$actual_revision"
printf 'IMAGE_VERSION=%s\n' "$actual_version"
