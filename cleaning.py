import sys
import glob

from pandas_schema import Column, Schema
import pandas_schema.validation as validation 
from pyspark import SparkConf, SparkContext, SparkFiles

from row import Row


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