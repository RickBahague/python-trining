#!/usr/bin/env python

'''
__author__ = "Rick Bahague"
__copyright__ = "Copyright 2016, Trining Python Utilities for PySpark and Local Data Science Work"
__credits__ = []
__license__ = "GPL"
__version__ = "3.0"
__maintainer__ = "Rick Bahague"
__email__ = "rick@cp-union.com"
__status__ = "Development"

'''

from datetime import datetime
import pandas as pd

ENV="NONE"

if(ENV=="SPARK"):
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    get_week_udf = udf(get_week, StringType())
    get_year_udf = udf(get_year, StringType())
    get_month_udf = udf(get_month, StringType())
    get_day_udf = udf(get_day, StringType())
    get_valid_sub = udf(get_valid_sub, StringType())

def get_day_seq():
    days_str = dict()
    days_str['Monday'] = 0
    days_str['Tuesday'] = 1
    days_str['Wednesday'] = 2
    days_str['Thursday'] = 3
    days_str['Friday'] = 4
    days_str['Saturday'] = 5
    days_str['Sunday'] = 6
    days_pdf = pd.DataFrame.from_dict(days_str,orient='index')
    days_pdf = days_pdf.reset_index()
    days_pdf.columns = ['Days',"Id"]
    return days_pdf


def get_week_start_end_dates(year, week):
    '''
    From http://stackoverflow.com/questions/30656008/get-start-date-and-end-date-of-the-week-given-week-number-and-year
    '''
    import datetime as datetime
    d = datetime.date(year,1,1)
    if(d.weekday()<= 3):
     d = d - datetime.timedelta(d.weekday())
    else:
     d = d + datetime.timedelta(7-d.weekday())
    dlt = datetime.timedelta(days = (week-1)*7)
    return d + dlt,  d + dlt + datetime.timedelta(days=6)

def get_week(date_str):
    import datetime
    date_from_str = datetime.datetime.strptime(date_str, '%Y%m%d')
    return datetime.datetime.strftime(date_from_str,'%U')

def get_month(date_str):
    import datetime
    date_from_str = datetime.datetime.strptime(date_str, '%Y%m%d')
    return datetime.datetime.strftime(date_from_str,'%m')

def get_year(date_str):
    import datetime
    date_from_str = datetime.datetime.strptime(date_str, '%Y%m%d')
    return datetime.datetime.strftime(date_from_str,'%Y')

def get_day(date_str):
    import datetime
    date_from_str = datetime.datetime.strptime(date_str, '%Y%m%d')
    return datetime.datetime.strftime(date_from_str,'%A')

def get_date_from_year_week_dayOfWeek(x):
    x = x.to_dict()
    day_str = str(x['year'])+str("-")+str(x['week'])+str("-")+str(x['day_str']+ " " +str(x['hour']))
    return datetime.strptime(day_str, '%Y-%U-%A %H')

def delimited_to_df(file,struct_list,delimiter,sc=sc,sqlContext=sqlContext):
    splitter = str(delimiter)
    rdd = sc.textFile(file)
    rdd = rdd.map(lambda x: x.encode("utf-8")).map(lambda x: x.split(splitter)).map(lambda x: tuple([xx.strip('"') for xx in x]))
    file_df = sqlContext.createDataFrame(rdd,struct_list)
    return file_df

if __name__ == "__main__":
    print "Trining Python Utilities for PySpark and Local Data Science Work"