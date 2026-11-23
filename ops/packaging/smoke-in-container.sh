#!/bin/sh
# Runs smoke.sh against a package in a fresh container booted with systemd
# (#2203). Needs Docker and a privileged container, as GitHub's runners allow.
#
#   ops/packaging/smoke-in-container.sh debian:12 dist/samyama_1.16.0-1_amd64.deb
#   ops/packaging/smoke-in-container.sh almalinux:9 dist/samyama-1.16.0-1.x86_64.rpm
set -eu

image=$1
pkg=$2
here=$(cd "$(dirname "$0")" && pwd)
name="samyama-smoke-$$"
base_image="${name}-base"
build_dir=$(mktemp -d)

case "$pkg" in
    *.deb) prep='apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y systemd systemd-sysv curl procps'
           init=/lib/systemd/systemd ;;
    # curl-minimal is already in the image; asking for curl conflicts with it.
    *.rpm) prep='dnf install -y systemd procps-ng'
           init=/usr/lib/systemd/systemd ;;
    *) echo "usage: $0 <image> <package.deb|package.rpm>" >&2; exit 2 ;;
esac

cleanup() {
    docker rm -f "$name" >/dev/null 2>&1 || true
    docker image rm "$base_image" >/dev/null 2>&1 || true
    rm -rf "$build_dir"
}
trap cleanup EXIT

# Prepare systemd before starting the init process. Installing packages in the
# container's startup command makes the readiness timeout include apt/dnf
# network and package work, which can outlast systemd's normal boot time.
printf 'FROM %s\nRUN %s\nCMD ["%s"]\n' "$image" "$prep" "$init" > "$build_dir/Dockerfile"
docker build --tag "$base_image" "$build_dir"

docker run -d --name "$name" --privileged --cgroupns=host \
    -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
    -v "$(cd "$(dirname "$pkg")" && pwd)":/pkg:ro -v "$here":/smoke:ro \
    "$base_image" >/dev/null

# Wait for systemd to come up as PID 1.
up=
for _ in $(seq 1 120); do
    state=$(docker exec "$name" /usr/bin/systemctl is-system-running 2>/dev/null || true)
    case "$state" in running|degraded) up=1; break ;; esac
    sleep 2
done
if [ -z "$up" ]; then
    echo "systemd did not start in $image (state: ${state:-none})" >&2
    docker logs "$name" 2>&1 | tail -20 >&2
    exit 1
fi

docker exec "$name" sh /smoke/smoke.sh "/pkg/$(basename "$pkg")"
