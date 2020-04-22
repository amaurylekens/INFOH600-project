import os
import csv
import sys
import glob
import json
import operator
from datetime import datetime

import pandas as pd
from pyspark import SparkConf, SparkContext, SparkFiles

from monthly_data import MonthlyDatum, MonthlyDataBucket
from row import Row
from utils import read_rows, construct_bucket

def time_difference(t1, t2):

    # build datetime object from string
    t1 = datetime.strptime(t1, '%Y-%m-%d %H:%M:%S')
    t2 = datetime.strptime(t2, '%Y-%m-%d %H:%M:%S')
    
    # compute time difference
    delta = t1-t2

    #compute total minutes in delta
    minutes = divmod(delta.total_seconds(), 60)[0]

    return minutes



# specify the columns for the different datasets
green_cost_columns = ['fare_amount', 'extra', 'mta_tax', 'tolls_amount', 'ehail_fee']
yellow_cost_columns = ['fare_amount', 'extra', 'mta_tax', 'tolls_amount', 'ehail_fee']
cost_columns = {'green': green_cost_columns,
                'yellow': yellow_cost_columns}

green_date_columns = {'pu' : 'lpep_pickup_datetime', 'do': 'lpep_dropoff_datetime'}
yellow_date_columns = {'pu': 'tpep_pickup_datetime', 'do': 'tpep_dropoff_datetime'}
date_columns = {'green': green_date_columns,
                'yellow': yellow_date_columns}

# spark configuration
sc = SparkContext()
sc.addFile("./analysis/monthly_data.py")
sc.addFile("./analysis/row.py")
sc.addFile("./analysis/utils.py")
sys.path.insert(0,SparkFiles.getRootDirectory())

# read all the filenames of the required dataset
datasets = ['green', 'yellow']
filenames = []
for dataset in datasets:
    filenames += sorted(glob.glob("./data/test-data/{}_*.csv".format(dataset)))

filenames = sc.parallelize(filenames)

# launch spark job
result = filenames.flatMap(lambda filename: read_rows(filename)) \
                  .map(lambda row: row.process()) \
                  .filter(lambda row: all([row.get(date) != "" 
                                           for date in date_columns[row.dataset].values()])) \
                  .map(lambda row: (row.filename, (row.sum(cost_columns[row.dataset]), 
                                                   row.get(date_columns[row.dataset]['do']),
                                                   row.get(date_columns[row.dataset]['pu'])))) \
                  .map(lambda pair: (pair[0], (pair[1][0], 
                                               time_difference(pair[1][1], 
                                                               pair[1][2])))) \
                   .reduceByKey(lambda t1, t2: tuple(map(operator.add, t1, t2))) \
                   .map(lambda pair: (pair[0], pair[1][0]/pair[1][1])) \
                   .map(lambda pair: construct_bucket(pair[0], pair[1])) \
                   .reduceByKey(lambda b1, b2 : MonthlyDataBucket.mix_buckets(b1, b2)) \
                   .map(lambda pair: pair[1].sort_by_date()) \
                   .map(lambda bucket: bucket.plot('./figures/average_minutes_cost')) \
                   .collect()

print(result)