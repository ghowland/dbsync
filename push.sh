#!/bin/bash

rsync -av * dbsync@dbsync:~/server/

rsync -av etc/init.d/* root@dbsync:/etc/init.d/

ssh root@dbsync "/etc/init.d/dbsync restart"

