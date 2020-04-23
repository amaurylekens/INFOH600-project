import os
import sys
import glob
import json
import subprocess

from row import Row

os.environ['HADOOP_CONF_DIR']="/etc/hadoop/conf"
os.environ['PYSPARK_PYTHON']="/usr/local/anaconda3/bin/python"
os.environ['PYSPARK_DRIVER_PYTHON']="/usr/local/anaconda3/bin/python"

from pyspark import SparkFiles
from pyspark.sql import SparkSession

try:
    spark
    print("Spark application already started")
    spark.stop()
except:
    pass

spark = SparkSession.builder \
                    .master("yarn") \
                    .config("spark.executor.instances", "4") \
                    .appName("Integration") \
                    .getOrCreate()

# spark configuration
sc = spark.sparkContext
sc.addFile("./transformations.py")
sc.addFile("./row.py")
sc.addFile("./shape_files/taxi_zones.shp")
sc.addFile("./shape_files/taxi_zones.dbf")
sys.path.insert(0,SparkFiles.getRootDirectory())

# load integration configuration
integration_confs = dict()
conf_filenames = sorted(glob.glob('/home/ceci18/INFOH600-project/integration_conf/*.json'))
for conf_filename in conf_filenames:    
    with open(conf_filename, 'r') as f:
        integration_conf = json.load(f)
        dataset_name = os.path.basename(conf_filename)[:-5]
        integration_confs[dataset_name] = integration_conf


# define path to save the file
input_path = './data'
output_path = './integrated'

#filenames = sorted(glob.glob("/home/hpda00034/infoh600/sampled/fhv_*.csv"))
filenames = ['fhv_tripdata_2015-06.csv']


# launch spark job
files = sc.wholeTextFiles(input_path)
rowsRDD = files.flatMap(lambda file: Row.read_rows(file)) \
               .map(lambda row: row.process()) \
               .map(lambda row: row.integrate(integration_confs[row.dataset])) \
               .persist()

for filename in filenames :
    rowsRDD.filter(lambda row : row.filename == filename) \
           .map(lambda row: row.data) \
           .coalesce(1) \
           .saveAsTextFile("{}/{}".format(output_path, filename[:-4]))
    
    subprocess.call(['hadoop', 'fs', '-rm', '{}/{}'.format(output_path, filename)]) 
    subprocess.call(['hadoop', 'fs', '-mv', '{}/{}/part-00000'.format(output_path, filename[:-4]), 
                         '{}/{}'.format(output_path, filename)])
    subprocess.call(['hadoop', 'fs', '-rm', '{}/{}/_SUCCESS'.format(output_path, filename[:-4])])
    subprocess.call(['hadoop', 'fs', '-rmdir', '{}/{}'.format(output_path, filename[:-4])])
    

try:
    spark.stop()
except:
    pass
