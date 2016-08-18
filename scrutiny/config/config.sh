#!/usr/bin/env bash

# DATE variable should be set before calling the script

# Send notifications to:
export MAIL_TO=atlstats@cern.ch

# SCRIPT path
export SCRIPTS_PATH=$BASE_DIR/scripts

export SCRIPTPATH=$BASE_DIR/scripts
export PIG_DIR=$BASE_DIR/scripts/pig
export SPARK_DIR=$BASE_DIR/scripts/spark_scripts

# Directories on HDFS (change it to yours)
# example: "/user/atlstats/scrutiny"
export HDFS_SCRUTINY_DIR="/user/inurgali/scrutiny"

# HDFS_TRACES_DIR must be with the same user as in scrutiny_util.py file (it is connection point between prepare and report processes)
export HDFS_TRACES_DIR="${HDFS_SCRUTINY_DIR}/traces-preprocess"
export HDFS_ZEROACCESS_DIR="${HDFS_SCRUTINY_DIR}/zeroaccess"

export HDFS_ALL_REPORTS_DIR="${HDFS_SCRUTINY_DIR}/reports"
export HDFS_REPORT_DIR=$HDFS_ALL_REPORTS_DIR/$DATE

# RUCIO dumps
export HDFS_RUCIO_DIR="/user/rucio01"
export HDFS_RUCIO_DUMPS_DIR="${HDFS_RUCIO_DIR}/dumps"

export POPULARITY_CSV_OUT_DIR=$HOME/www/scrutiny/$DATE/csv

export ZEROACCESS_OUT_DIR=$HOME/www/zeroaccess/$DATE
export ZEROACCESS_OUT_LIST=$ZEROACCESS_OUT_DIR/list-$DATE
export ZEROACCESS_CSV_OUT_DIR=$ZEROACCESS_OUT_DIR/csv

# log folder
export LOG_DIR=$HOME/private/scrutiny/logs

# select spark mod
EXEC_MOD=local # local, yarn-cluster, yarn-client