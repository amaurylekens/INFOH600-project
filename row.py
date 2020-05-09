import os
import csv

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pyspark import SparkFiles


class Row():
    @staticmethod
    def validate(df, validation_schema):

        """
        Validate the dataframe and return infos of the errors

        :param df: df to validate
        :param validation_schema: schema to validate the data 
        :return: list with errors info
        """

        # validate the data
        errors = validation_schema.validate(df)
        errors = [[error.row, error.column, error.value] for error in errors]

        return errors

    
    @staticmethod
    def integrate(data, schema, integration_conf, zones):

        """
        Transforms data into the desired schema 
        by following the configuration

        :param integration_conf: dict with the configuration
        """

        data = dict(zip(schema, data))
        t_data = dict() # transformed data

        # loop through all the columns of the last schema
        for column, alias_list in integration_conf.items():
            found = False

            # loop through all alias (column or function)
            for alias in alias_list:
                category = alias['type']
                content = alias['content']

                # check category of the alias
                # the alias is an other column name
                if category == 'column':

                    if content in list(data.keys()):
                        t_data[column] = data[content]
                        found = True
                        break

                # the alias is a function with other column name as param
                elif category == 'function':

                    func_name = content['func_name']
                    param_names = content['params']
                    params = []

                    # check that all the params are in the schema of the data
                    eval_func = True
                    for param_name in param_names:
                        if param_name not in list(data.keys()):
                            eval_func = False
                        else:
                            params.append(data[param_name])
                    
                    # eval the function if all the params are there
                    if eval_func and func_name == "compute_location_id" :
                        not_valid = ["0", ""]
                        if not(params[0] in not_valid or params[1] in not_valid):
                            
                            # recuperate params
                            long = float(params[0])
                            lat = float(params[1])
                            
                            rtree = zones.sindex
                            # find possible match for the point
                            pnt = Point(long,lat)
                            possible_matches = list(rtree.intersection(pnt.bounds))

                            # find the right zone
                            for m in possible_matches:
                                if zones.iloc[m].geometry.contains(pnt):
                                    t_data[column] = m
                            found=True

            # if there is no valid alias add empty data
            if not found:
                t_data[column] = ''

        data = list(t_data.values())

        return data
    
    @staticmethod
    def process(data):
    
        data = data.replace('\n', '').replace('"','').replace("'", '')
        data = data.split(',')
        return data
    
    @staticmethod
    def join(data):
        
        data = ','.join([str(entry) for entry in data])
        
        return data


    @staticmethod
    def read_rows(lines, begin, latest, path):

        """
        Construct row Objects from a tuple
        (path, (*rows))

        :param file: tuple (path, (*rows))
        :return: a list of Row object
        """

        filename = os.path.basename(path)
        dataset = filename.split('_', 1)[0]

        rows = []

        # read schema (first line of the file)
        schema = lines[0]
        lines = lines[1:]

        for line in lines[begin:latest]:
            row = Row(schema, line, filename, dataset)
            rows.append(row)

        return rows