#!/bin/sh
# Installs a samyama .deb or .rpm and checks it as a service (#2203, CH-DEPLOY).
#
# Runs inside a container whose PID 1 is systemd, as root:
#
#   ops/packaging/smoke-in-container.sh debian:12 dist/samyama_1.16.0-1_amd64.deb
#
# Checks, in order: the package installs with its dependencies; it creates the
# samyama account and does not start or enable the service by itself; the
# service starts under its hardening as that account; a write survives a
# restart; removal stops the service and keeps the data.
set -eu

# systemd as PID 1 may not preserve the image's PATH for docker exec shells.
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export PATH

pkg=$1
fail() { echo "FAIL: $*" >&2; journalctl -u samyama --no-pager -n 50 >&2 || true; exit 1; }
ok() { echo "ok: $*"; }

case "$pkg" in
    *.deb) install() { apt-get install -y -qq "$pkg" >/dev/null; }
           remove() { apt-get remove -y -qq samyama >/dev/null; } ;;
    *.rpm) install() { dnf install -y -q "$pkg" >/dev/null; }
           remove() { dnf remove -y -q samyama >/dev/null; } ;;
    *) echo "usage: $0 <package.deb|package.rpm>" >&2; exit 2 ;;
esac

query() {
    curl -sf -X POST http://127.0.0.1:8080/api/query \
        -H 'Content-Type: application/json' -d "{\"query\":\"$1\"}"
}
wait_up() {
    for _ in $(seq 1 60); do
        curl -sf http://127.0.0.1:8080/api/status >/dev/null 2>&1 && return 0
        sleep 1
    done
    fail "no answer on :8080/api/status after 60 s"
}

install
ok "installed $(basename "$pkg")"
getent passwd samyama >/dev/null || fail "no samyama account"
ok "samyama account exists"
[ -f /etc/samyama/samyama.env ] || fail "no /etc/samyama/samyama.env"
systemctl is-active --quiet samyama && fail "the service started on install"
systemctl is-enabled --quiet samyama && fail "the service was enabled on install"
ok "not started or enabled on install"
systemd-analyze verify /usr/lib/systemd/system/samyama.service || fail "unit does not verify"
ok "unit verifies"

systemctl enable --now samyama
wait_up
user=$(ps -o user= -p "$(systemctl show -p MainPID --value samyama)")
[ "$user" = samyama ] || fail "service runs as '$user', not samyama"
ok "service runs as samyama"
[ "$(stat -c %U /var/lib/samyama)" = samyama ] || fail "/var/lib/samyama is not owned by samyama"

query "CREATE (:Smoke {n: 42})" >/dev/null || fail "write refused"
systemctl restart samyama
wait_up
query "MATCH (s:Smoke) RETURN s.n AS n" | grep -q 42 || fail "the write did not survive a restart"
ok "a write survives a restart"
echo "exposure: $(systemd-analyze security samyama --no-pager | tail -1)"

remove
systemctl is-active --quiet samyama && fail "still running after removal"
[ -d /var/lib/samyama ] || fail "removal deleted /var/lib/samyama"
ok "removed; service stopped, data kept"
echo "PASS $(basename "$pkg")"
