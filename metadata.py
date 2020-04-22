
import glob
import pandas as pd

from pyspark import SparkContext
import numpy as np

from utils import get_metadata



sc = SparkContext()

import os

def get_schema(filename):
    '''Extracts the schema from the given file
    
    Assumes that the first line of the file includes the schema
    '''
    with open(filename, 'r') as f:
        return [attr.strip('" ').lower() for attr in f.readline().strip().split(',')]
      
def get_month(filename):
    '''Returns the month that the TCL file reports on.
    
       Assumes that the filename uses the TLC convensions:
       $(fileSource)_tripdata_$(year)-$(month).csv
    '''
    return int(filename[-6:-4])

def get_year(filename):
    '''Returns the month that the TCL file reports on. 
    
       Assumes that the filename uses the TLC convensions:
       $(fileSource)_tripdata_$(year)-$(month).csv
    '''

    return int(filename[-11:-7])

def get_type(filename):
    '''Returns the type of trip that the TCL file reports on (yellow, green, fhv, hvfhv). 
    
       Assumes that the filename uses the TLC convensions:
       $(fileSource)_tripdata_$(year)-$(month).csv
    '''

    # The basename of the file is the file name without the folder information
    # E.g., if filename = "/home/stijn/foo.txt" then the basename = "foo.txt"
    basename = os.path.basename(filename)
    # compute the type here from the basename string
    transport_class = basename.split('_', 1)[0]
    return transport_class 

def get_numrecords(filename):
    '''Returns the number of records in a TCL file.
       
       Equals the number of lines in the file minus one 
       (the header, which is the schema, not a record)
    '''
    with open(filename) as f:
        lines = 0
        for line in f:
            lines += 1
        return lines - 1                   
    
def get_metadata(filename):
    '''Returns all metadata associated to the `filename` datafile as one big tuple'''
    return (filename, 
            get_type(filename),
            get_year(filename),
            get_month(filename),
            os.path.getsize(filename),
            get_numrecords(filename),
            get_schema(filename) )    


# compute dataframe (parallelization on the file)
files = sorted(glob.glob("/home/amaury/code/sampled/*.csv"))
files = sc.parallelize(files)

metadata = files.map(lambda file: get_metadata(file)).collect()

# Put the metadata in a Pandas dataframe
metadata_labels = [ 'filename',  'type', 'year', 'month', 'size', 'num_records', 'schema']
df = pd.DataFrame.from_records(metadata, columns=metadata_labels)

df.to_json('/home/amaury/code/dataset-description.json')
