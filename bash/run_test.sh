#!/bin/bash

LOG=/etc/clickhouse/scheduler.log
STEP=/opt/etl-scheduler/scripts/step.sh
CONFIG_PATH=/etc/clickhouse/config.xml

printf '%(%Y-%m-%d %H:%M:%S)T\tINFO\tscheduler started\n' >> $LOG

process() {
  export CLC="clickhouse-client -C $CONFIG_PATH --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD -h $4 -n -f TSV --param_topic=${1}_p"
  if ! ps ax | grep "${CLC%%#*}" | grep -v grep > /dev/null
  then
    printf '%(%Y-%m-%d %H:%M:%S)T\tINFO\t'"$1-$3"'\tstarted\n' >> $LOG
    echo  "$2" | $STEP  "$1" "$3" &
  fi
}

while true ; do
  clickhouse-client -C $CONFIG_PATH --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD -q "select * from SCH.LagLive" -f TSVRaw | \
  while IFS=$'\t' read -r topic host sql ts version; do
    if ! ps ax | grep "${topic%%#*}" | grep -v grep > /dev/null
    then
      process $topic "$sql" $version $host
    else
      continue
    fi
  done
  sleep 1
done