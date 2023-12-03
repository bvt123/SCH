#!/bin/bash

LOG=$HOME/scheduler.log
#LOG=/dev/stdout

err=`$CLC --log_comment=$HID:$2 "--param_upto=${3}" 3>&1 1>>$LOG 2>&3 | grep Exception | tr "'" " " | sed -e 's/.*Exception: |\(.*\)|: while.*/\1/'`

if [ "$err" != "" ] ;  then

        printf '%(%Y-%m-%d %H:%M:%S)T\tWARN\t'"$1-$2"'\t'"$err"'\n' >> $LOG
#        printf "insert into ETL.ErrLog(topic,err) values(\'$1\',\'$err\')" | $CLC 2>> $LOG
        printf "insert into SCH.Offsets select topic,last,rows,next,processor,\'$err\',hostid from SCH.Offsets where topic=\'$1\'" |
                $CLC 2>> $LOG

fi
sleep 1

