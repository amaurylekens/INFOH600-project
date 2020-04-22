import os
import csv
import sys
import glob
import json

import pandas as pd
from pyspark import SparkConf, SparkContext, SparkFiles

from monthly_data import MonthlyDatum, MonthlyDataBucket
from row import Row
from utils import read_rows, construct_bucket
 

zones = pd.read_csv('data/zones.csv')

man = list(zones.loc[zones['Borough']=='Manhattan', 'LocationID'])
bro = list(zones.loc[zones['Borough']=='Brooklyn', 'LocationID'])

# spark configuration
sc = SparkContext()
sc.addFile("./analysis/monthly_data.py")
sc.addFile("./analysis/row.py")
sc.addFile("./analysis/utils.py")
sys.path.insert(0,SparkFiles.getRootDirectory())

# read all the filenames
filenames = sorted(glob.glob("./data/test-data/*.csv"))
filenames = sc.parallelize(filenames)

# launch spark job
result = filenames.flatMap(lambda filename: read_rows(filename)) \
                  .map(lambda row: row.process()) \
                  .filter(lambda row: (((int(row.get('pulocationid')) in man)  
                                     & (int(row.get('dolocationid')) in man)) 
                                     |((int(row.get('pulocationid')) in bro)
                                     & (int(row.get('dolocationid')) in bro)))) \
                  .map(lambda row: (row.filename, 1)) \
                  .reduceByKey(lambda count1, count2: count1 + count2) \
                  .map(lambda pair: construct_bucket(pair[0], pair[1])) \
                  .reduceByKey(lambda b1, b2 : MonthlyDataBucket.mix_buckets(b1, b2)) \
                  .map(lambda pair: (pair[0], pair[1].sort_by_date())) \
                  .map(lambda pair: pair[1].plot('./figures/trip-count-man-bro')) \
                  .collect()

print(result)
