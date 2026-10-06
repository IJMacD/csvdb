#!/bin/sh
set -e
case "${1:-}" in
  "" ) exec /startup.sh ;;
  csvdb|sh|ash|lighttpd|/startup.sh|/usr/bin/csvdb) exec "$@" ;;
  *) exec csvdb "$@" ;;
esac
