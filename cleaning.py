import os
import sys
import csv
import glob

import pandas as pd
from pandas_schema import Column, Schema
import pandas_schema.validation as validation 
from pyspark import SparkConf, SparkContext, SparkFiles


def read_rows(filename):

    """
    Read a .csv file aand return a list of 
    rows with the schema and the filename

    :param filename: the path of a .csv file with the rows 
    :return: list of dict - schema: column names of the files
                          - data: entries of the row
                          - filename: name of the file 
    """

    f = open(filename, 'r')
    rows = []

    # read schema (first line of the file)
    schema = f.readline()
    data = f.readline()

    while data:
        rows.append({'schema': schema, 
                     'data': data, 
                     'filename': filename})
        data = f.readline()

    return rows

def validate_row(row, validation_schema):
    
    """
    Validate the entries of the row with
    the schema

    :param row: dict {'schema': ..., 'data': ..., 
                      'filename': ...}
    :param validation_schema: schema to validate the data 
    :return: a tuple (filename, {'data': [], 
                                 'validated': bool})
    """
    
    # split and clean the string to a list
    data = row['data'].replace('\n', '').split(',')
    schema = row['schema'].replace('\n', '').lower().split(',')

    # validate the data
    df = pd.DataFrame([data], columns=schema)
    errors = validation_schema.validate(df)
    
    if errors == []:
        validated = True   
    else:
        validated = False

    validated_row = {'data': data,
                     'schema': schema,
                     'validated': validated}

    return (row['filename'], [validated_row])


def save_validated_rows(rows):

    """
    Write a list of rows in the specified file

    :param rows: tuple (filename, [*rows])
    """

    filename = os.path.basename(rows[0])
    f = open('./data/test-data/cleaned/{}'.format(filename), 'w')
    with f:
        writer = csv.writer(f)

        schema = rows[1][0]['schema']
        writer.writerow(schema)
        for row in rows[1]:
            if row['validated']:
                writer.writerow(row['data'])
    

# validation schema for fhv dataset
schema_fhv = Schema([
    Column('dispatching_base_num', 
           [validation.MatchesPatternValidation('^B[0-9]{5}$')], 
           allow_empty=True),
    Column('pickup_datetime', 
           [validation.DateFormatValidation('%Y-%m-%d %H:%M:%S')],
           allow_empty=True),
    Column('dropoff_datetime', 
           [validation.DateFormatValidation('%Y-%m-%d %H:%M:%S')],
           allow_empty=True),
    Column('pulocationid',
           [validation.InRangeValidation(1, 266)],
           allow_empty=True),
    Column('dolocationid', 
           [validation.InRangeValidation(1, 266)],
           allow_empty=True),
    Column('sr_flag', 
           [validation.InListValidation([1, None])],
           allow_empty=True)
])


# validation schema for fhvhv dataset
schema_fhvhv = Schema([
    Column('hvfhs_license_num', 
           [validation.MatchesPatternValidation('^HV[0-9]{4}$')], 
           allow_empty=True),
    Column('dispatching_base_num', 
           [validation.MatchesPatternValidation('^B[0-9]{5}$')], 
           allow_empty=True),
    Column('pickup_datetime', 
           [validation.DateFormatValidation('%Y-%m-%d %H:%M:%S')],
           allow_empty=True),
    Column('dropoff_datetime', 
           [validation.DateFormatValidation('%Y-%m-%d %H:%M:%S')],
           allow_empty=True),
    Column('pulocationid',
           [validation.InRangeValidation(1, 266)],
           allow_empty=True),
    Column('dolocationid', 
           [validation.InRangeValidation(1, 266)],
           allow_empty=True),
    Column('sr_flag', 
           [validation.InListValidation([1, None])],
           allow_empty=True)
])


# validation schema for green dataset
schema_green = Schema([
    Column('vendorid', 
           [validation.InListValidation([1, 2])], 
           allow_empty=True),
    Column('lpep_pickup_datetime', 
           [validation.DateFormatValidation('%Y-%m-%d %H:%M:%S')],
           allow_empty=True),
    Column('lpep_dropoff_datetime', 
           [validation.DateFormatValidation('%Y-%m-%d %H:%M:%S')],
           allow_empty=True),
    Column('store_and_fwd_flag', 
           [validation.InListValidation(["Y", "N"])], 
           allow_empty=True),
    Column('ratecodeid', 
           [validation.InListValidation([1, 2, 3, 4, 5, 6])], 
           allow_empty=True),
    Column('pulocationid',
           [validation.InRangeValidation(1, 266)],
           allow_empty=True),
    Column('dolocationid', 
           [validation.InRangeValidation(1, 266)],
           allow_empty=True),
    Column('passenger_count', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('trip_distance', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('fare_amount', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('extra', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('mta_tax', 
           [validation.InListValidation([0.5, 0])], 
           allow_empty=True),
    Column('tip_amount', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('tolls_amount', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('ehail_fee', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('improvement_surcharge', 
           [validation.InListValidation([0.3, 0])], 
           allow_empty=True),
    Column('total_amount', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('payment_type', 
           [validation.InListValidation([1, 2, 3, 4, 5, 6])], 
           allow_empty=True),
    Column('trip_type', 
           [validation.InListValidation([1, 2])], 
           allow_empty=True),
    Column('congestion_surcharge', 
           [validation.InRangeValidation(min=0)],
           allow_empty=True)
])

# validation schema for yellow dataset
schema_yellow = Schema([
    Column('vendorid', 
           [validation.InListValidation([1, 2])], 
           allow_empty=True),
    Column('tpep_pickup_datetime', 
           [validation.DateFormatValidation('%Y-%m-%d %H:%M:%S')],
           allow_empty=True),
    Column('tpep_dropoff_datetime', 
           [validation.DateFormatValidation('%Y-%m-%d %H:%M:%S')],
           allow_empty=True),
    Column('store_and_fwd_flag', 
           [validation.InListValidation(["Y", "N"])], 
           allow_empty=True),
    Column('ratecodeid', 
           [validation.InListValidation([1, 2, 3, 4, 5, 6])], 
           allow_empty=True),
    Column('pulocationid',
           [validation.InRangeValidation(1, 266)],
           allow_empty=True),
    Column('dolocationid', 
           [validation.InRangeValidation(1, 266)],
           allow_empty=True),
    Column('passenger_count', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('trip_distance', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('fare_amount', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('extra', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('mta_tax', 
           [validation.InListValidation([0.5, 0])], 
           allow_empty=True),
    Column('tip_amount', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('tolls_amount', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('improvement_surcharge', 
           [validation.InListValidation([0.3, 0])], 
           allow_empty=True),
    Column('total_amount', 
           [validation.InRangeValidation(min=0)], 
           allow_empty=True),
    Column('payment_type', 
           [validation.InListValidation([1, 2, 3, 4, 5, 6])], 
           allow_empty=True),
    Column('congestion_surcharge', 
           [validation.InRangeValidation(min=0)],
           allow_empty=True)
])

schemas = {'fhv' : schema_fhv,
           'fhvhv': schema_fhvhv, 
           'green': schema_green,
           'yellow': schema_yellow}


sc = SparkContext()
sc.addFile("./transformations.py")
sys.path.insert(0,SparkFiles.getRootDirectory())

dataset = "fhv"
# read all the filenames of the dataset
filenames = sorted(glob.glob("./data/test-data/integrated/{}_*.csv".format(dataset)))
filenames = sc.parallelize(filenames)

# recuperate the the shcema validation of the dataset
validation_schema = schemas[dataset]

# launch spark job
data = filenames.flatMap(lambda filename: read_rows(filename)) \
                .map(lambda row: validate_row(row, validation_schema)) \
                .reduceByKey(lambda row_1, row_2 : row_1 + row_2) \
                .map(lambda rows: save_validated_rows(rows)) \
                .collect() 

         
