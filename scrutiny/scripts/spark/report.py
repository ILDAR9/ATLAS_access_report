#!/bin/env python

from scrutiny_util import *

from  pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import functions as F

import time
import logging

# LOGGER =  logging.getLogger(LOGGER_NAME + ".report")

# column names
SCOPE = 'scope'
ID = 'id'
NAME = 'name'
BYTES = 'bytes'
RSE = 'rse'
RSE_ID = 'rse_id'
OP_EVENTS = 'op_events'
CREATED_AT = 'created_at'
TRANSIENT = 'transient'
UUID = 'uuid'
FILEOPS = 'fileops'
TIMEENTRYUNIX = 'timeentryunix'
ACCESS_NUM = 'access_num'
VOLUME = 'volume'

sqlCtx, LOGGER = prepareCtxAndLogger('preprocess_report_zeroacccess')

@logDF()
def restore_traces():
    """
    SCHEMA
     |-- scope: string (nullable = true)
     |-- name: string (nullable = true)
     |-- uuid: string (nullable = true)
     |-- fileops: long (nullable = false)
     |-- timeentry: double (nullable = true)

     -- Rest of the traces
        '/user/rucio01/tmp/processed_traces_30_07_2015/*'
    -- Preprocessed traces up to Aug 2016
        'scrutiny/traces-preprocess/*/*'
    traces = UNION traces1, traces2;
    """
    trace_schema = StructType([ \
        StructField(SCOPE, StringType(), True), \
        StructField(NAME, StringType(), True), \
        StructField(UUID, StringType(), True), \
        StructField(FILEOPS, LongType(), False), \
        StructField(TIMEENTRYUNIX, DoubleType(), True)])

    load_trace = lambda trace_path: sqlCtx.load(source="com.databricks.spark.csv", header='true', schema=trace_schema,
                                                path=trace_path, delimiter='\t')

    return load_trace(envs['HDFS_TRACE_2015']).unionAll(load_trace(envs['HDFS_PREPROCESS']))


@logDF()
def prepare_dids():
    '''
     (transient IS NULL) AND (NOT (name rlike '.*_sub.*')) AND ( (scope rlike '^data.*') OR (scope rlike '^mc.*') OR (scope rlike '^valid.*') )
    '''
    return mod_avro(sqlCtx).load(envs['HDFS_DIDS']). \
        select(*lower_case_columns(CREATED_AT, SCOPE, NAME, TRANSIENT)). \
        filter(F.isnull(TRANSIENT) & ~col(NAME).rlike('.*_sub.*') & (col(SCOPE).rlike('^data.*') | col(SCOPE).rlike('^mc.*') | col(SCOPE).rlike('^valid.*'))). \
        select((col(CREATED_AT) / 1000).alias(CREATED_AT).astype(DoubleType()), SCOPE, NAME)


@logDF()
def prepare_disk_replicas():
    rses = mod_avro(sqlCtx).load(envs['HDFS_RSES']).select(col('ID').alias(ID).astype(StringType()), *lower_case_columns(RSE))
    dslocks = mod_avro(sqlCtx).load(envs['HDFS_DSLOCKS']).select(col('RSE_ID').alias(RSE_ID).astype(StringType()),
                                                         *lower_case_columns(SCOPE, NAME, BYTES))
    return dslocks.join(rses, dslocks.rse_id == rses.id). \
        select(col(BYTES).astype(LongType()), SCOPE, NAME, RSE). \
        filter(col(RSE).rlike('.*DATADISK'))


@logDF()
def prepare_primary_datadisk():
    get_datadisk_replicas = prepare_disk_replicas()
    dids = prepare_dids()

    return dids.join(get_datadisk_replicas, [SCOPE, NAME]). \
        groupBy(SCOPE, NAME, RSE).agg(F.max(BYTES).alias(BYTES), F.min(CREATED_AT).alias(CREATED_AT))


@logDF()
def count_events(traces_time=None):
    count_not_null = lambda c, s: F.sum(col(c).isNotNull().cast("integer")).alias(s)
    filt_cond = col(TIMEENTRYUNIX) > traces_time if traces_time else ~F.isnull(TIMEENTRYUNIX)

    return restore_traces(). \
        filter(filt_cond). \
        groupBy(SCOPE, NAME).agg(count_not_null(UUID, OP_EVENTS)). \
        select(SCOPE, NAME, OP_EVENTS)


@logDF()
def get_dataset_volume(traces_time):
    get_size_primary_datadisk = prepare_primary_datadisk()
    ev_count = count_events(traces_time)

    g = get_size_primary_datadisk.alias('g')
    c = ev_count.alias('c')
    return get_size_primary_datadisk.join(ev_count, (g.scope == c.scope) & (g.name == c.name), how='left_outer'). \
        select(g.scope.alias(SCOPE), g.name.alias(NAME), RSE, BYTES,
               F.when(F.isnull(OP_EVENTS), F.when(col(CREATED_AT) < traces_time, -1).otherwise(0)).otherwise(col(OP_EVENTS)).alias(OP_EVENTS)). \
        groupBy(SCOPE, NAME).agg(F.max(OP_EVENTS).alias(OP_EVENTS), F.sum(BYTES).alias(BYTES))


@logDF()
def prepare_ordered_accesscount(traces_time):
    return get_dataset_volume(traces_time).groupBy(OP_EVENTS).agg(F.sum(BYTES).alias(VOLUME)). \
        orderBy(OP_EVENTS, ascending=True). \
        select(col(OP_EVENTS).alias(ACCESS_NUM), VOLUME)


get_month = lambda month: '%02d' % month if month > 0 else "infinity"


def process_period(month, out_folder):
    # 30 days with 86400 seconds in a day
    traces_time = month * 30 * DAY_SECONDS

    _time_prep_start = time.time()
    cur_report = prepare_ordered_accesscount(traces_time)
    _time_prep_end = time.time()
    LOGGER.info('Spark scripts preparation time = %.3f seconds' % (_time_prep_end - _time_prep_start))
    # HDFS_REPORT_DIR
    cur_report.write.format('com.databricks.spark.csv').save('%s/%s.csv' % (out_folder, get_month(month)), format='csv', delimiter=',', mode='overwrite', header='true')
    LOGGER.info("Collection TIME for REPORT = %.3f seconds" % (time.time() - _time_prep_end))


@prepare_decor()
def main():
    out_folder = get_args(0)
    periods = [3, 6, 9, 12, 0]
    _time_start = time.time()
    for month in periods:
        _time_period_process_start = time.time()
        LOGGER.info("period of (%s) months START" % get_month(month))
        process_period(month, out_folder)
        LOGGER.info("period of (%s) months END. %3.f seconds" % (
            get_month(month), time.time() - _time_period_process_start))
    LOGGER.info("Averall REPORT TIME = %.3f seconds" % (time.time() - _time_start))


if __name__ == "__main__":
    main()
