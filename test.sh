#!/bin/bash


#export SCHEDULE=/l/ipython/Schedule
#export GOOGLE=/c/Google/py
export PYTHONP=/c/ProgramData/Anaconda3/python

#export BASEDIR=/c/Users/miyuk_000/Documents/Miyuki/windows
export BASEDIR=/l/ipython
export SCHEDULE=$BASEDIR/Schedule
export GOOGLE=$BASEDIR/Google/py
export LOGS=$SCHEDULE/daily.log

echo "------------------------------------" > $LOGS
echo "check dynamodb up and runnuing" >> $LOGS

cd $SCHEDULE
$PYTHONP $SCHEDULE/countTableSchedular.py >> $LOGS
export FAILED=$SCHEDULE/dynamo_connectionfail

if [ -f $FAILED ]; then
    echo "Sorry, dynamodb is NOT running.." >> $LOGS
    echo "Program ends." >> $LOGS
    exit 0
else
    echo "dynamodb is up and running.." >> $LOGS
    echo "..." >> $LOGS
fi

