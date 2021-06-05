#!/bin/sh


#export SCHEDULE=/l/ipython/Schedule
#export GOOGLE=/c/Google/py
export PYTHONP=/c/ProgramData/Anaconda3/python

#export BASEDIR=/c/Users/miyuk_000/Documents/Miyuki/windows
export BASEDIR=/l/ipython
export SCHEDULE=$BASEDIR/Schedule
export GOOGLE=$BASEDIR/Google/py
export LOGS=$SCHEDULE/dailyAllScheduleYOGA.log


echo "------------------------------------" > $LOGS
#echo "check dynamodb up and runnuing" >> $LOGS

#cd $SCHEDULE
#$PYTHONP $SCHEDULE/countTableSchedular.py >> $LOGS
#export FAILED=$SCHEDULE/dynamo_connectionfail

#if [ -f $FAILED ]; then
#    echo "Sorry, dynamodb is NOT running.." >> $LOGS
#    echo "Program ends." >> $LOGS
#    exit 0
#else
#    echo "dynamodb is up and running.." >> $LOGS
#    echo "..." >> $LOGS
#fi


#echo "------------------------------------" > $LOGS
#echo "[makeAllSchedule.sh] START to create CSV from GoogleSheet" >> $LOGS
#cd $GOOGLE
#$PYTHONP $GOOGLE/createLGCSVFromSheet.py >> $LOGS

#echo "[makeAllSchedule.sh] insert GoogleSheet into MongoDB(cloud) " >> $LOGS
#cd $SCHEDULE
#echo "[makeAllSchedule.sh]  batch process to insert data  " >> $LOGS
#$PYTHONP $SCHEDULE/insertLGSchedularBatch.py >> $LOGS
#$PYTHONP $SCHEDULE/insertMongoLGSchedularBatch.py >> $LOGS

echo "[makeAllSchedule.sh] build integrate2.csv " >> $LOGS
#$PYTHONP $SCHEDULE/buildIntegrate2CSVForScheduleSheet.py >> $LOGS
#$PYTHONP $SCHEDULE/buildMongoIntegrate2CSVForScheduleSheet.py >> $LOGS

$PYTHONP $SCHEDULE/buildMongoIntegrate2CSVForScheduleSheetTobuNERIMA_v3.py >> $LOGS
$PYTHONP $SCHEDULE/buildOrderEpiCSV.py >> $LOGS

#echo "------------------------------------" >> $LOGS
#echo "[makeAllSchedule.sh] Append RECEPTY new data on Google Sheet." >> $LOGS
#cd $GOOGLE
#$PYTHONP $GOOGLE/appendScheduleSheet.py >> $LOGS
#$PYTHONP $GOOGLE/clearAndSetFilterSortScheduleSheet.py >> $LOGS
