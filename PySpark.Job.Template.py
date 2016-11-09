#!/usr/bin/env python

"""A PySpark Job Template"""

__author__ = "Rick Bahague"
__copyright__ = "Copyright 2016, Trining Python Utilities for PySpark and Local Data Science Work"
__credits__ = []
__license__ = "GPL"
__version__ = "3.0"
__maintainer__ = "Rick Bahague"
__email__ = "rick@cp-union.com"
__status__ = "Development"

import os, csv, time, sys
from datetime import datetime, timedelta, date
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark import SparkFiles
from StringIO import StringIO
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

sc.addFile("hdfs://<HDFS_PATH_OF_FILE_YOU_WANT_DISTRIBUTED_ON_THE_JOB")

#Define Spark Job Settings
conf = SparkConf()
conf.setAppName("PYSPARK_JOB_NAME")
conf.set('spark.hadoop.mapreduce.output.fileoutputformat.compress', 'false')
conf.set('spark.sql.parquet.compression.codec','uncompressed')
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

def function01(args):
    #your python code here

def function02(args):
    #your python code here

def job_pipeline(args):
    function01(args)
    function02(args)

if __name__ == "__main__":
    task = sys.argv[1]
    other_args = sys.argv[2]
    start_time = time.time()
    run={"function01" : function01, "function02" : function02, "job_pipeline": job_pipeline}
    run[task](other_args)
    sc.stop()
    print("--- %s seconds ---" % (time.time() - start_time))
    print "Job Done"