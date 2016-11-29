1) Zeppelin

part 1) Scrutiny report visualisation by 3, 6, 9, 12 and infinity periods for 0 - 14 and more accesses.

part 2) Scrutiny report visualisation by selected last X months showing TOP-N unused dataset names.
 
2) Scrutiny report
ref to https://github.com/ATLAS-Analytics/scrutiny , there we replace PIG script to Spark works
# Data usage statistics for ATLAS DDM
Main reports:
* Volumes of data vs. number of accesses for last N months
* The volume of datasets as s function of number of times accessed in periods of 3, 6, 9, 12 months and infinity
* List of never used datasets
* Detailed statistics on unused data by project and datatype

Usage:

_./process.sh_

# Configuration
Set-up working folders in config.sh and scrutiny_util.py files: set-in your user-name of analytix server to store report results

If you plan use local mod for execution of Spark scripts, then you should execute script 'loac_conf.sh' from 'hdfs_remote_condf' folder. 
Afterwards set 'conf' folder as HADOOP_CONF_DIR environment.

As the next step, set up Hadoop libs environments for hadoop 'csv' and 'avro' libs search.

export HROOT="${HADOOP_HOME}/share/hadoop"

export HADOOP_LIBS=\`echo ${HROOT}/common/hadoop-common-[0-9]*[0-9].jar\`

export HADOOP_LIBS="${HADOOP_LIBS};"\`echo ${HROOT}/hdfs/hadoop-hdfs-[0-9]*[0-9].jar\`

export HADOOP_LIBS="${HADOOP_LIBS};"\`echo ${HROOT}/common/lib/*.jar | sed 's/ /;/g'\`

# Data sources

# Output
