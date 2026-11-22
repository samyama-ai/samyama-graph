#!/bin/sh
# (#2203) Stops the service when the package is removed, not when it is
# upgraded: .deb passes "remove", .rpm passes 0 on removal and 1 on upgrade.
set -e
if [ "$1" = "remove" ] || [ "$1" = "0" ]; then
    if [ -d /run/systemd/system ]; then
        systemctl disable --now samyama.service >/dev/null 2>&1 || true
    fi
fi
