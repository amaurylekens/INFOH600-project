import os
import csv
import sys
import glob
import json
import operator

import pandas as pd
from pyspark import SparkConf, SparkContext, SparkFiles

from monthly_data import MonthlyDatum, MonthlyDataBucket
from row import Row
from utils import read_rows, construct_bucket


# specify the columns for the different datasets

tip_column = {'green': 'tip_amount',
              'yellow': 'tip_amount'}

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
                  .map(lambda row: (row.filename, (float(row.get(tip_column[row.dataset])), 1))) \
                  .reduceByKey(lambda t1, t2: tuple(map(operator.add, t1, t2))) \
                  .map(lambda pair: (pair[0], pair[1][0]/pair[1][1])) \
                  .map(lambda pair: construct_bucket(pair[0], pair[1])) \
                  .reduceByKey(lambda b1, b2 : MonthlyDataBucket.mix_buckets(b1, b2)) \
                  .map(lambda pair: pair[1].sort_by_date()) \
                  .map(lambda bucket: bucket.plot('./figures/average-tip')) \
                  .collect()