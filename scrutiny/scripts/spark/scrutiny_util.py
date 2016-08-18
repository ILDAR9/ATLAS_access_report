###############
# CONFIGURATION
###############

# spark app name
APP_NAME = 'scrutiny_report'

# CONSTS
DAY_SECONDS = 86400
USER_NAME = 'inurgali' #Analytix, change it to yours
LOGGER_NAME = "Scrutiny"

# working envs
envs = {}

def _load_envs(DATE):
    # preprocess traces
    envs['HDFS_TRACE'] = "/user/rucio01/traces/traces.%s*[0-9]" % DATE
    envs['HDFS_CONTENTS'] = "/user/rucio01/dumps/%s/contents/*.avro" % DATE

    # report
    envs['HDFS_TRACE_2015'] = '/user/rucio01/tmp/processed_traces_30_07_2015/*'
    envs['HDFS_DIDS'] = '/user/rucio01/dumps/%s/dids' % DATE
    envs['HDFS_RSES'] = '/user/rucio01/dumps/%s/rses' % DATE
    envs['HDFS_DSLOCKS'] = '/user/rucio01/dumps/%s/dslocks' % DATE

    # HDFS_PREPROCESS must be the same as in conf.sh (it is connection point between prepare and report processes)
    envs['HDFS_PREPROCESS'] = '/user/%s/scrutiny/traces-preprocess/*/*' % USER_NAME

    return True



################
# UTIL FUNCTINOS
################
import os, sys
import logging
from pyspark.sql.functions import col

def quiet_logs(sc):
    """
    LOGGER LEVEL change to ERROR from INFO
    """
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger('pyspark').setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger(LOGGER_NAME).setLevel(logger.Level.DEBUG)

LOGGER = None

def prepareCtxAndLogger(log_name):
    from pyspark import SparkContext, SQLContext
    sc = SparkContext(appName=APP_NAME)
    quiet_logs(sc)
    global LOGGER
    LOGGER =  sc._jvm.org.apache.log4j.LogManager.getLogger(LOGGER_NAME + '.' + log_name)
    return SQLContext(sc), LOGGER


def lower_case_columns(*colNames):
    """
     generate columns aliases with lower case
    """
    if type(colNames[0]) is list: colNames = colNames[0]
    is_mixed_case = not reduce(lambda cum, x: cum & x.islower(), colNames, True)
    return map(lambda x: col(x if is_mixed_case else x.upper()).alias(x.lower()), colNames)


"""
AVRO file
"""
mod_avro = lambda sqlCtx: sqlCtx.read.format("com.databricks.spark.avro")


def logDF():
    """
    Decorator for dataframe creating methods to log a schema
    expects: function returning Dataframe
    """
    def decorate(func):
        def call(*args, **kwargs):
            r = func(*args, **kwargs)
            LOGGER.debug(' %s\n%s' % (func.__name__, r._jdf.schema().treeString()))
            return r

        return call

    return decorate


def get_args(index=None):
    """
    Generalisaed args for spark python scripts
    :return: tuple(out_file, DATE)
    """
    args = sys.argv[1:]
    if (not args or len(args) < 2 or (not args[0]) or (not args[1])):
        print "Args: out_file_path date_of_report"
        exit(1)
    if index is not None:
        return args[index]
    else:
        out_file = args[0]
        DATE = args[1]
        return (out_file, DATE)


def get_conf(conf_key):
    return envs[conf_key]


def prepare_decor():
    """
    Decorator for main method: conf loading, args checking
    """
    def decorate(func):
        def func_wrap(*args, **kwargs):
            # check args
            DATE = get_args(1)
            # check confs
            if not _load_envs(DATE):
                exit(1)
            # start main
            func(*args, **kwargs)

        return func_wrap

    return decorate
