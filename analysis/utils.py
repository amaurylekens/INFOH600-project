import os

from row import Row
from monthly_data import MonthlyDatum, MonthlyDataBucket


def construct_bucket(filename, count):

    """
    Create a bucket with one MonthlyDatum objet

    :param filename: the path of a .csv file  
    :return: a key-value pair (dataset_name, bucket)
    """

    month = int(filename[-6:-4])
    year = int(filename[-11:-7])
    dataset_name = os.path.basename(filename).split('_', 1)[0]

    monthly_datum = MonthlyDatum(count, year, month)
    bucket = MonthlyDataBucket(dataset_name, 'trip count (Man/Bro)', monthly_datum)
    
    return (dataset_name, bucket)
