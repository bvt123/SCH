#!/bin/bash

LOG=$HOME/scheduler.log
#LOG=/dev/stdout
STEP=$HOME/scheduler/step.sh
HID=`hostname`-`hostid`

printf '%(%Y-%m-%d %H:%M:%S)T\tINFO\tscheduler started\n' >> $LOG

process() {
    export CLC="clickhouse-client -h $4 -n -f TSV --param_topic=${1}_p"
    if ! ps ax | grep "$CLC" | grep -v grep > /dev/null
    then
        printf '%(%Y-%m-%d %H:%M:%S)T\tINFO\t'"$1-$3"'\tstarted\n' >> $LOG
        echo  "$2" | $STEP  "$1" "$3" &
    fi
}

while true ; do
  clickhouse-client -q  "select * from SCH.Tasks" -f TSVRaw | \
  while IFS=$'\t' read -r topic host sql ts version; do
       process $topic "$sql" $version $host
  done
  sleep 1
done
