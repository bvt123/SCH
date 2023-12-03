#!/bin/bash

LOG=$HOME/scheduler.log
#LOG=/dev/stdout
STEP=$HOME/scheduler/step.sh
export HID=`hostname`-`hostid`

printf '%(%Y-%m-%d %H:%M:%S)T\tINFO\tscheduler started\n' >> $LOG

process() {
    export CLC="clickhouse-client -n -f TSV --param_topic=${1}_p"   # -h $4 for shard processing
    if ! ps ax | grep "$CLC" | grep -v grep > /dev/null
    then
        printf '%(%Y-%m-%d %H:%M:%S)T\tINFO\t'"$1-$3"'\tstarted\n' >> $LOG
        echo  "$5" | $STEP  "$1" "$2" "$3" &
    fi
}

while true ; do
  clickhouse-client -q  "select * from SCH.Tasks" -f TSVRaw | \
  while IFS=$'\t' read -r topic host sql upto ts version hostid; do
    if [[ -z "$hostid" ]] || [[ "$hostid" == "$HID" ]]; then
       process "$topic" "$version" "$upto" $host "$sql"
    else
      printf '%(%Y-%m-%d %H:%M:%S)T\tWARNING\t$topic served on $hostid\n' >> $LOG
    fi
  done
  sleep 1
done
