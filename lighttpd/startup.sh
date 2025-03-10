#!/bin/sh

trap cleanup HUP INT QUIT TERM

cleanup() {
  # stop service and clean up here
  echo "stopping lighttpd"
  kill -HUP $(cat /var/run/lighttpd.pid)

  # stop tail
  kill %1

  echo "exited $0"

  exit
}

CONFFILE=/etc/lighttpd/lighttpd.conf
LOGFILE=$(grep ^accesslog.filename $CONFFILE | awk '{print $NF}' | tr -d '"')

touch $LOGFILE

# start streaming log to stdout
tail -f $LOGFILE &

# start service in background here
lighttpd -f $CONFFILE

echo "started lighttpd"

while true; do
  sleep 1;
done
