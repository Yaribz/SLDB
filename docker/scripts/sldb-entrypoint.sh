#!/bin/bash

LOBBY_PORT="${LOBBY_PORT:=8200}"
XMLRPC_LISTEN_ADDR="${XMLRPC_LISTEN_ADDR:=0.0.0.0}"
XMLRPC_LISTEN_PORT="${XMLRPC_LISTEN_PORT:=8300}"

SLDB_DB_USER="${SLDB_DB_USER:-}"
SLDB_DB_PASSWORD="${SLDB_DB_PASSWORD:-}"
SLDB_DB_DATABASE="${SLDB_DB_DATABASE:-}"
LOBBY_LOGIN="${LOBBY_LOGIN:-}"
LOBBY_PASSWORD="${LOBBY_PASSWORD:-}"

[ -z "$SLDB_DB_USER" ] && echo "WARN: SLDB_DB_USER missing"
[ -z "$SLDB_DB_PASSWORD" ] && echo "WARN: SLDB_DB_PASSWORD missing"
[ -z "$SLDB_DB_DATABASE" ] && echo "WARN: SLDB_DB_DATABASE missing"
[ -z "$LOBBY_LOGIN" ] && echo "WARN: LOBBY_LOGIN missing"
[ -z "$LOBBY_PASSWORD" ] && echo "WARN: LOBBY_PASSWORD missing"
[ -z "$MONITOR_LOBBY_PASSWORD" ] && echo "WARN: MONITOR_LOBBY_PASSWORD missing"

find /etc/sldb -name "*.conf" -exec sh -c '/opt/replace-vars.sh {} "/sldb/etc/$(basename {})"' \;

touch /sldb/log/$LOGFILE.log

echo "Running $@ at $(printf '%s %s\n' "$(date)")" >> /sldb/log/$LOGFILE.log

exec $@ 2>&1 | tee -a /sldb/log/$LOGFILE.log
