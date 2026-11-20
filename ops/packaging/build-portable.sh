#!/bin/sh
# Builds target/release/samyama on AlmaLinux 9, so it runs on glibc 2.34 and
# newer (#2203): RHEL/Rocky/Alma 9, Amazon Linux 2023, Debian 12, Ubuntu
# 22.04/24.04. AlmaLinux 9 is the oldest of those and supported to 2032;
# Debian 11, older still, is past end of life and its archive is gone.
#
# A binary needs the glibc of the system it was linked on, and
# built on ubuntu-24.04 it needed 2.39: it ran on Ubuntu 24.04 and on nothing
# older (the service crash-looped on Debian 12 with "GLIBC_2.38 not found").
#
# Runs on the host's architecture. Needs Docker. Then checks the result:
# exits non-zero if the binary needs a glibc newer than GLIBC_MAX.
#
#   ops/packaging/build-portable.sh
set -eu

GLIBC_MAX=2.34
cd "$(dirname "$0")/../.."

docker run --rm -v "$PWD":/src -w /src -e CARGO_TERM_COLOR=always \
    -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" almalinux:9 bash -c '
set -euo pipefail
dnf install -y -q --enablerepo=crb gcc gcc-c++ make cmake pkgconf-pkg-config \
    openssl-devel clang clang-devel >/dev/null
curl -sSf --retry 5 --retry-all-errors https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable -q
. "$HOME/.cargo/env"
cargo build --release --bin samyama
chown -R "$HOST_UID:$HOST_GID" target
'

need=$(objdump -T target/release/samyama | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -uV | tail -1)
echo "target/release/samyama needs glibc $need (at most $GLIBC_MAX allowed)"
if [ "$(printf '%s\n%s\n' "$need" "$GLIBC_MAX" | sort -V | tail -1)" != "$GLIBC_MAX" ]; then
    echo "error: needs glibc $need, newer than $GLIBC_MAX" >&2
    exit 1
fi
