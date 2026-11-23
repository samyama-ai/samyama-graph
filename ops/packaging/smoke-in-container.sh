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

case "$pkg" in
    *.deb) prep='apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq systemd systemd-sysv curl procps >/dev/null'
           init=/lib/systemd/systemd ;;
    # curl-minimal is already in the image; asking for curl conflicts with it.
    *.rpm) prep='dnf install -y -q systemd procps-ng >/dev/null'
           init=/usr/lib/systemd/systemd ;;
    *) echo "usage: $0 <image> <package.deb|package.rpm>" >&2; exit 2 ;;
esac

cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$name" --privileged --cgroupns=host \
    -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
    -v "$(cd "$(dirname "$pkg")" && pwd)":/pkg:ro -v "$here":/smoke:ro \
    "$image" sh -c "$prep && exec $init" >/dev/null

# Wait for systemd to come up as PID 1.
up=
for _ in $(seq 1 120); do
    # Resolve systemctl directly; systemd may not leave it on docker exec's PATH.
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
