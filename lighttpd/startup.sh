#!/bin/sh

CONFFILE=/etc/lighttpd/lighttpd.conf
LOGFILE=$(grep ^accesslog.filename $CONFFILE | awk '{print $NF}' | tr -d '"')
touch $LOGFILE
tail -f $LOGFILE &
lighttpd -D -f $CONFFILE