import os
import csv
import sys
import glob
import json

from pyspark import SparkConf, SparkContext, SparkFiles

from monthly_data import MonthlyDatum, MonthlyDataBucket

def count_trips(filename):

    """
    Create a bucket with one MonthlyDatum objet
    from a .csv file

    :param filename: the path of a .csv file  
    :return: a key-value pair (dataset_name, bucket)
    """

    month = int(filename[-6:-4])
    year = int(filename[-11:-7])
    dataset_name = os.path.basename(filename).split('_', 1)[0]

    f = open(filename)
    trip_count = sum(1 for row in f)-1

    monthly_datum = MonthlyDatum(trip_count, year, month)
    bucket = MonthlyDataBucket(dataset_name, 'trip count', monthly_datum)
    
    return (dataset_name, bucket)   


sc = SparkContext()
sc.addFile("./analysis/monthly_data.py")
sys.path.insert(0,SparkFiles.getRootDirectory())

# read all the filenames
filenames = sorted(glob.glob("./data/tlc-integrated/*.csv"))
filenames = sc.parallelize(filenames)

# launch spark job
result = filenames.map(lambda filename: count_trips(filename)) \
                  .reduceByKey(lambda b1, b2 : MonthlyDataBucket.mix_buckets(b1, b2)) \
                  .map(lambda pair: (pair[0], pair[1].sort_by_date())) \
                  .map(lambda pair: pair[1].plot('./figures/trip-count/')) \
                  .collect()
