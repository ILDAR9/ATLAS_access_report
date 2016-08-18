#!/bin/env python
from scrutiny_util import *

from pyspark.sql.types import *
from pyspark.sql.functions import col
import pyspark.sql.functions as F
import time, sys

sqlCtx, LOGGER = prepareCtxAndLogger('preprocess_traces')

# -----------------------------------rucio trace--------------------------------------------------------
def prepare_trace():
    """
    LOADS /user/rucio01/traces/traces.2016-*[0-9]
    """
    # SCHEMA predefine
    schemaString = "traceTimeentryUnix timeStart timeEnd dataset filename hostname scope localSite remoteSite ip eventType clientState uuid usrdn".split()
    fields = [StructField(field_name, DoubleType(), True) for field_name in schemaString[:3]]
    fields = fields + [StructField(field_name, StringType(), True) for field_name in schemaString[3:]]
    schema = StructType(fields)

    HDFS_TRACE = envs['HDFS_TRACE']
    sqlCtx.read.schema(schema).json(HDFS_TRACE). \
        select(col('traceTimeentryUnix').alias('timeentryunix'), *lower_case_columns(schemaString[1:])). \
            filter(~(F.isnull('filename') | F.isnull('eventtype') | F.isnull('timeentryunix'))).\
                registerTempTable("traces_valid")


# ----------------------------------contents avro-----------------------------------------------------------
def prepare_content():
    """
    LOADS /user/rucio01/dumps/$DATE/contents'
    """
    schemaString = "SCOPE NAME CHILD_NAME DID_TYPE CHILD_TYPE"
    HDFS_CONTENTS = envs['HDFS_CONTENTS']
    mod_avro(sqlCtx).load(HDFS_CONTENTS). \
        select(lower_case_columns(schemaString.split())). \
            filter((col('did_type') == 'D') & (col('child_type') == 'F')).\
                registerTempTable("content")

def prepare_trace_content():
    prepare_trace()
    prepare_content()
    # -- scope, name, eventtype, uuid from join by file_name and child_name
    sqlCtx.sql("SELECT D1.scope, D1.name, t.eventtype, t.uuid, t.timeentryunix FROM traces_valid AS t JOIN content AS D1 ON (t.filename=D1.child_name) "
                               "WHERE D1.scope rlike '^(data|ms|valid).*$' AND t.eventtype rlike '^(get.*|download)$' ").\
        registerTempTable("D")

@logDF()
def prepare():
    prepare_trace_content()
    # -- GROUP BY (scope, name, uuid)
    return sqlCtx.sql("SELECT scope, name, uuid, Count(*) AS fileops, MAX(timeentryunix) AS timeentryunix FROM D GROUP BY scope, name, uuid ORDER BY timeentryunix ASC")

def execute_preprocess(out_file):
    _time_prep_start = time.time()
    G = prepare()
    _time_prep_end = time.time()
    LOGGER.info("Preparation TIME = %.3f seconds" % (_time_prep_end - _time_prep_start))
    # HDFS_PREPROCESS
    G.write.format('com.databricks.spark.csv').save(out_file, format='csv', delimiter='\t', mode='overwrite', header='true')

    _time_collect_end = time.time()
    LOGGER.info("Collection TIME = %.3f seconds" % (_time_collect_end - _time_prep_end))
    LOGGER.info("Averall TIME = %.3f seconds" % (_time_collect_end - _time_prep_start))

@prepare_decor()
def main():
    out_file = get_args(0)
    LOGGER.info("OUT_FILE path" + out_file)
    execute_preprocess(out_file)

if __name__ == "__main__":
    main()