# !/bin/env python

from scrutiny_util import *

from  pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from report import prepare_primary_datadisk, count_events
import time, datetime

from report import LOGGER
# Column names
from report import OP_EVENTS, SCOPE, CREATED_AT, RSE, BYTES, NAME
AGE_DAYS = 'age_days'

@logDF()
def get_dataset_volume():
    get_size_primary_datadisk = prepare_primary_datadisk()
    ev_count = count_events()

    g = get_size_primary_datadisk.alias('g')
    c = ev_count.alias('c')
    return get_size_primary_datadisk.join(ev_count, (g.scope == c.scope) & (g.name == c.name), how='left_outer'). \
        select(g.scope.alias(SCOPE), g.name.alias(NAME), RSE, BYTES, CREATED_AT,
               F.when(F.isnull(OP_EVENTS), 0).otherwise(col(OP_EVENTS)).alias(OP_EVENTS)). \
        groupBy(SCOPE, NAME).agg(F.max(OP_EVENTS).alias(OP_EVENTS), F.sum(BYTES).alias(BYTES), F.min(CREATED_AT).alias(CREATED_AT))


@logDF()
def prepare_zeroaccess(time_utc):
    return get_dataset_volume(). \
        filter(col(OP_EVENTS) == 0). \
        select(SCOPE, NAME, BYTES, col(CREATED_AT).astype(IntegerType()), ((time_utc - col(CREATED_AT)) / DAY_SECONDS).astype(IntegerType()).alias(AGE_DAYS)). \
        orderBy(SCOPE, NAME)


def process_zeroaccess(out_folder, DATE):
    date_utc = datetime.datetime.strptime(DATE, '%Y-%m-%d')
    time_utc = time.mktime(date_utc .timetuple()) + DAY_SECONDS

    _time_prep_start = time.time()
    cur_report = prepare_zeroaccess(time_utc)
    _time_prep_end = time.time()
    LOGGER.info('Spark scripts preparation time = %.3f seconds' % (_time_prep_end - _time_prep_start))
    # HDFS_ZEROACCESS_DIR
    cur_report.write.format('com.databricks.spark.csv').save('%s/list-%s.csv' % (out_folder, DATE), format='csv', delimiter='\t', mode='overwrite', header='true')
    LOGGER.info("Collection TIME for ZEROACCESS = %.3f seconds" % (time.time() - _time_prep_end))


@prepare_decor()
def main():
    out_folder, DATE = get_args()
    process_zeroaccess(out_folder, DATE)


if __name__ == "__main__":
    main()