import sys
import glob
import json

from pyspark import SparkConf, SparkContext, SparkFiles

from row import Row

# spark configuration
sc = SparkContext()
sc.addFile("./transformations.py")
sys.path.insert(0,SparkFiles.getRootDirectory())

# load integration configuration
filename = './integration_conf/integration_fhv.json'
with open(filename, 'r') as f:
    integration_conf = json.load(f)

# read all the filenames
filenames = sorted(glob.glob("./data/test-data/fhv_*.csv"))
filenames = sc.parallelize(filenames)

# define path to save the file
path = './data/integrated'

# launch spark job
result = filenames.flatMap(lambda filename: Row.read_rows(filename)) \
         .map(lambda row: row.process()) \
         .map(lambda row: row.integrate(integration_conf)) \
         .map(lambda row: (row.filename, [row])) \
         .reduceByKey(lambda row_1, row_2 : row_1 + row_2) \
         .map(lambda pair: Row.save_rows(pair[1], path)) \
         .collect()
