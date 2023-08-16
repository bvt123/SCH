#!/bin/bash

LOG=./scheduler.log
REQ=./sql-debug-cache/$1.sql
HID=$(echo `hostname` | cut -d '-' -f -2)
delimiter='{set_insert_deduplication_token}'

echo set agi_topic=\'$1\'\; > $REQ
cat >> $REQ
SQL=`cat $REQ`

if grep $delimiter $REQ  > /dev/null ; then
  token=`echo "${SQL%$delimiter*}" | $CLC  "--log_comment=$HID:$2" 2>>$LOG`

  if [ "$token" == "" ] ; then
    printf '%(%Y-%m-%d %H:%M:%S)T\tERROR\t'"$1-$2"'\tcan not get deduplication_token\n' >> $LOG
    exit 1
  fi
  token_param=" --insert_deduplication_token=$token"
fi

err=`echo "${SQL#*$delimiter}" | $CLC "--log_comment=$HID:$2" $token_param 3>&1 1>>$LOG 2>&3 | grep Exception | tr "'" " " | sed -e 's/.*Exception: |\(.*\)|: while.*/\1/'`

if [ "$err" != "" ] ;  then

  printf '%(%Y-%m-%d %H:%M:%S)T\tWARN\t'"$1-$2"'\t'"$err"'\n' >> $LOG

  printf "insert into ETL.ErrLog(topic,err) values(\'$1\',\'$err\')" | $CLC 2>> $LOG

  printf "insert into SCH.Offsets select topic,last,rows,next,processor,\'$err\',hostid from SCH.Offsets where topic=\'$1\'" |
    $CLC 2>> $LOG

fi
sleep 2
