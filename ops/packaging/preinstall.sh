#!/bin/sh
# Creates the account the service runs as (#2203). Same script for .deb and .rpm.
set -e
if ! getent group samyama >/dev/null; then
    groupadd --system samyama
fi
if ! getent passwd samyama >/dev/null; then
    useradd --system --gid samyama --home-dir /var/lib/samyama --no-create-home \
        --shell /usr/sbin/nologin --comment "Samyama graph database" samyama
fi
