import os
import csv

import pandas as pd

class Row():
    def __init__(self, schema, data, filename, dataset):
        self.schema = schema            # schema of the row
        self.data = data                # entries of the row
        self.filename = filename        # filename of the row
        self.dataset = dataset          # dataset name of the row
        self.schema_processed = False   # false if schema is not list         
        self.data_processed = False     # false if data is not list
       
        if type(self.schema) == list:
            self.schema_processed = True

        if type(self.data) == list:   
            self.data_processed = True
    
    
    def get(self, column):

        """
        Get the entry of a desired column 
        of the row
        
        :param column: the name of the column
        :return: the desired entry
        """

        i = self.schema.index(column)
        return self.data[i]
        

    def process(self):

        """
        Process the schema and the data of the row 
        if not already done
        """

        if not self.schema_processed:
            if type(self.schema) == str:
                self.schema = self.schema.replace('\n', '')
                self.schema = self.schema.lower().split(',')
                self.schema_processed = True

        if not self.data_processed:
            if type(self.data) == str:
                self.data = self.data.replace('\n', '')
                self.data = self.data.split(',')
                self.data_processed = True

        return self


    def sum(self, columns):

        """
        Compute the sum of values of many columns 

        :param columns: list of columns on which 
                        the summation is performed  
        :return: the sum of the values
        """

        return sum([float(self.get(column)) for column in columns
                    if self.get(column) != ""])


    def integrate(self, integration_conf):

        """
        Transforms data into the desired schema 
        by following the configuration

        :param integration_conf: dict with the configuration
        """

        data = dict(zip(self.schema, self.data))
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
                    if eval_func:
                        import transformations as t
                        eval_string = "t.{}({})".format(func_name, ','.join(map(str, params)))
                        t_data[column] = exec(eval_string)
                        found = True

            # if there is no valid alias add empty data
            if not found:
                t_data[column] = ''
            
        self.data = list(t_data.values())
        self.schema = list(t_data.keys())

        return self

    def validate(self, validation_schema):
    
        """
        Validate the entries of the row with
        the schema
 
        :param validation_schema: schema to validate the data 
        :return: boolean, true if validated
        """

        # validate the data
        df = pd.DataFrame([self.data], columns=self.schema)
        errors = validation_schema.validate(df)
        
        validated = len(errors) == 0

        return validated


    @staticmethod
    def read_rows(file):

        """
        Construct row Objects from a tupple 
        representing a file

        :param path: path of the file to read
        :return: a list of Row object
        """

        path = file[0]
        filename = os.path.basename(path)
        dataset = filename.split('_', 1)[0]
        lines = file[1].split("\n")
        rows = []

        # read schema (first line of the file)
        schema = lines[0]
        lines = lines[1:]

        for line in lines:
            row = Row(schema, line, filename, dataset)
            rows.append(row)

        return rows

    @staticmethod
    def save_rows(rows, path, sc):

        """
        Write a list of rows in the specified file

        :param rows: list of Row object
        :param path: path to the .csv file to record the row
        :param sc: spark context
        """

        filename = (rows[0].filename)
        f = open('./{}'.format(filename), 'w')
        with f:
            writer = csv.writer(f)
            schema = rows[0].schema
            writer.writerow(schema)
            for row in rows:
                writer.writerow(row.data)
