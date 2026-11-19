#!/bin/sh
# Installs the samyama snap and checks it under strict confinement (#2203,
# CH-DEPLOY). Run on a host with snapd, as a user with sudo:
#
#   ops/packaging/snap-smoke.sh samyama_1.16.0_amd64.snap
#
# Checks, in order: it installs strict; the service answers; a write lands in
# $SNAP_COMMON; a .sgsnap exported from it loads back; the data survives a
# restart; a refresh (a second revision) keeps the data and copies none of it
# into the new revision; `snap revert` goes back with the data intact; the CLI
# runs; and AppArmor denied nothing on the way.
set -eu

snap=$1
URL=http://127.0.0.1:8080
fail() { echo "FAIL: $*" >&2; sudo snap logs samyama -n 80 >&2 || true; exit 1; }
ok() { echo "ok: $*"; }
query() {
    curl -sf -X POST "$URL/api/query" -H 'Content-Type: application/json' -d "{\"query\":\"$1\"}"
}
wait_up() {
    for _ in $(seq 1 120); do
        curl -sf "$URL/api/status" >/dev/null 2>&1 && return 0
        sleep 1
    done
    fail "no answer on $URL/api/status after 120 s"
}
smoke_count() {
    query "MATCH (s:Smoke) RETURN count(s) AS c" | sed -n 's/.*"records":\[\[\([0-9]*\)\]\].*/\1/p'
}
revision() { snap list samyama | awk 'NR == 2 { print $3 }'; }
since=$(date '+%Y-%m-%d %H:%M:%S')

sudo snap install --dangerous "$snap"
snap info --verbose samyama | grep -q '^confinement: *strict' || fail "not strict confinement"
ok "installed strict, revision $(revision)"
wait_up
snap services samyama | grep -q 'samyama.daemon .*enabled .*active' || fail "service not enabled and active"
ok "samyama.daemon is enabled and active"

query "CREATE (:Smoke {n: 42})" >/dev/null || fail "write refused"
sudo test -d /var/snap/samyama/common/data || fail "no data directory under \$SNAP_COMMON"
ok "a write is stored under /var/snap/samyama/common/data"

tmp=$(mktemp -d)
curl -sf -X POST "$URL/api/snapshot/export" -o "$tmp/smoke.sgsnap" || fail "export failed"
[ -s "$tmp/smoke.sgsnap" ] || fail "empty .sgsnap"
before=$(smoke_count)
curl -sf -X POST "$URL/api/snapshot/import" -F "file=@$tmp/smoke.sgsnap" >/dev/null || fail "import failed"
after=$(smoke_count)
[ "${after:-0}" -gt "${before:-0}" ] || fail "the .sgsnap loaded nothing ($before -> $after)"
ok "a .sgsnap exported from it loads back ($before -> $after :Smoke)"

sudo snap restart samyama
wait_up
[ "$(smoke_count)" = "$after" ] || fail "data lost on restart"
ok "the data survives a restart"

first=$(revision)
sudo snap install --dangerous "$snap"
second=$(revision)
[ "$second" != "$first" ] || fail "no new revision after the second install"
wait_up
[ "$(smoke_count)" = "$after" ] || fail "data lost on refresh"
copied=$(sudo du -sk "/var/snap/samyama/$second" | cut -f1)
common=$(sudo du -sk /var/snap/samyama/common | cut -f1)
[ "$copied" -lt 1024 ] || fail "refresh copied ${copied} KB into /var/snap/samyama/$second"
ok "refresh $first -> $second kept the data in \$SNAP_COMMON (${common} KB); ${copied} KB in the new revision"

sudo snap revert samyama
[ "$(revision)" = "$first" ] || fail "revert did not return to $first"
wait_up
[ "$(smoke_count)" = "$after" ] || fail "data lost on revert"
ok "snap revert back to $first, data intact"

samyama --help >/dev/null || fail "the samyama CLI does not run"
ok "the samyama CLI runs"

denied=$(sudo journalctl -k --since "$since" --no-pager 2>/dev/null | grep -c 'apparmor="DENIED".*snap\.samyama' || true)
[ "$denied" = 0 ] || { sudo journalctl -k --since "$since" --no-pager | grep 'apparmor="DENIED".*snap\.samyama' | head -20 >&2; fail "$denied AppArmor denials"; }
ok "no AppArmor denials"
echo "PASS $(basename "$snap")"
