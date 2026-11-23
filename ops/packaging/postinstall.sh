#!/bin/sh
# (#2203) Tells systemd about the unit. Does not enable or start it: the
# database listens on the network, and that is the administrator's call.
set -e
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    # On an upgrade, a running service picks up the new binary.
    systemctl try-restart samyama.service >/dev/null 2>&1 || true
fi
if [ "$1" = "configure" ] && [ -z "$2" ] || [ "$1" = "1" ]; then
    echo "samyama installed. Start it with: sudo systemctl enable --now samyama"
    echo "Options: /etc/samyama/samyama.env  Data: /var/lib/samyama"
fi
