#!/bin/sh
# (#2203) The graph in /var/lib/samyama and the samyama account are kept: a
# package removal must never delete a database. Remove them by hand.
set -e
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi
