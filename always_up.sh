#!/bin/bash
#
# Ensure dbsync service is always up
#

/etc/init.d/dbsync status > /dev/null 2>&1

if [ $? -ne 0 ] ; then
  /etc/init.d/dbsync restart
fi

