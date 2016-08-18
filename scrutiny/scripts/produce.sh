#!/usr/bin/env bash

YEAR=`echo $DATE | cut -f 1 -d "-"`

cd $LOGS # Some logs are being written to the current directory

# Preprocess traces
hadoop fs -mkdir -p "${HDFS_TRACES_DIR}" >/dev/null 2>&1
hadoop fs -rm -r "${HDFS_TRACES_DIR}/${YEAR}_fresh" >/dev/null 2>&1

# set-up appropriate settings for execution
if [[ $EXEC_MOD == "yarn-cluster" || $EXEC_MOD == "yarn-client" ]]; then
    LOG=`spark-submit \
          --master $EXEC_MOD \
          --py-files spark/scrutiny_util.py \
          --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0 \
          --num-executors 2 \
          --driver-memory 3g \
          --executor-memory 4g \
           spark/preprocess_traces.py \
          ${HDFS_TRACES_DIR}/${YEAR}_fresh \
          $DATE`
elif [ $EXEC_MOD == "local" ]; then
    LOG=`spark-submit \
          --master local[\*] \
          --py-files spark/scrutiny_util.py \
          --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0 \
          --num-executors 2 \
          --driver-memory 5g \
           spark/preprocess_traces.py \
          ${HDFS_TRACES_DIR}/${YEAR}_fresh \
          $DATE`
else
    echo "EXEC_MOD: local, yarn-client, yarn_cluster [CHOOSE!].";
    exit $?;
fi
CODE=$?

# --------------------Preprocessing ^ collects in crutiny

if [ $CODE -eq 0 ]; then
    echo "Done" | mail -s "Scrutiny: traces preprocessing finished" $MAIL_TO
    echo $LOG
else
    echo "$LOG" | mail -s "Scrutiny: traces preprocessing failed" $MAIL_TO
    echo $LOG
    exit $CODE
fi

# Replace preprocesed traces
hadoop fs -rm -r "${HDFS_TRACES_DIR}/${YEAR}" >/dev/null 2>&1
hadoop fs -rm -r "${HDFS_TRACES_DIR}/${YEAR}" >/dev/null 2>&1
hadoop fs -mv "${HDFS_TRACES_DIR}/${YEAR}_fresh" "${HDFS_TRACES_DIR}/${YEAR}"

# ---------------------------Replace updates ^

# Prepare report directories
hadoop fs -rm -r "$HDFS_REPORT_DIR" >/dev/null 2>&1
hadoop fs -mkdir -p "$HDFS_REPORT_DIR" >/dev/null 2>&1

# report
if [[ $EXEC_MOD == "yarn-cluster" || $EXEC_MOD == "yarn-client" ]]; then
    LOG=`spark-submit \
          --master $EXEC_MOD \
          --py-files spark/scrutiny_util.py \
          --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0 \
          --num-executors 2 \
          --driver-memory 3g \
          --executor-memory 4g \
           spark/report.py \
          ${HDFS_REPORT_DIR} \
          $DATE`
elif [ $EXEC_MOD == "local" ]; then
    LOG=`spark-submit \
          --master local[\*] \
          --py-files spark/scrutiny_util.py \
          --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0 \
          --num-executors 2 \
          --driver-memory 5g \
           spark/report.py \
          ${HDFS_REPORT_DIR} \
          $DATE`
else
    echo "EXEC_MOD: local, yarn-client, yarn_cluster [CHOOSE!].";
    exit $?;
fi

if [ $CODE -eq 0 ]; then
    echo "Done" | mail -s "Scrutiny zeroaccess: finished for zeroaccess" $MAIL_TO
else
    echo "$LOG" | mail -s "Scrutiny zeroaccess: falied for zeroaccess" $MAIL_TO
fi

# Zero-access statistics prepare
if [[ $EXEC_MOD == "yarn-cluster" || $EXEC_MOD == "yarn-client" ]]; then
    LOG=`spark-submit \
          --master $EXEC_MOD \
          --py-files spark/scrutiny_util.py,spark/report.py \
          --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0 \
          --num-executors 2 \
          --driver-memory 3g \
          --executor-memory 4g \
           spark/zeroaccess.py \
          ${HDFS_ZEROACCESS_DIR} \
          $DATE`
elif [ $EXEC_MOD == "local" ]; then
    LOG=`spark-submit \
          --master local[\*] \
          --py-files spark/scrutiny_util.py,spark/report.py \
          --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0 \
          --num-executors 2 \
          --driver-memory 5g \
           spark/zeroaccess.py \
          ${HDFS_ZEROACCESS_DIR} \
          $DATE`
else
    echo "EXEC_MOD: local, yarn-client, yarn_cluster [CHOOSE!].";
    exit $?;
fi

CODE=$?

if [ $CODE -eq 0 ]; then
    echo "Done" | mail -s "Scrutiny: finished for zeroaccess" $MAIL_TO
else
    echo "$LOG" | mail -s "Scrutiny: falied for zeroaccess" $MAIL_TO
fi

echo $LOG

exit $CODE
