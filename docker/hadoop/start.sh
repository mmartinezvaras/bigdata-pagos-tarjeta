#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
 namenode)
   mkdir -p /data/name
   if [ ! -f /data/name/current/VERSION ]; then
     if [ -n "$(ls -A /data/name)" ]; then
       echo 'Directorio NameNode no vacio sin VERSION: revisar; no se formatea.' >&2
       exit 1
     fi
     hdfs namenode -format -nonInteractive
   fi
   exec hdfs namenode ;;
 datanode) mkdir -p /data/data; exec hdfs datanode ;;
 *) exec "$@" ;;
esac
