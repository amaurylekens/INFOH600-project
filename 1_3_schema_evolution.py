import pandas as pd

def diff_schema(schema1, schema2):
    ''' Compute a tuple containing all elements of schema1 that are not in schema2        
    
        Example: if  schema1= ("a", "b", "c") and schema2 = ("b", "d", "e") the result = ("a", "c")
    '''
    lschema1 = list(schema1) 
    lschema2 = list(schema2)

    removed = [x for x in lschema1 if x not in lschema2 ]

    return tuple(removed) # removed is a list, convert it back to a tuple



def analyze_schema_changes(dataset):
    '''Analyze schema changes over time for all files in the dataset
    
    dataset: A dataframe that lists all files beloning to a given sub-dataset (fhv, yellow, green, ...)
             with their metadata, sorted lexicographically on (year, month)
    
    output: a dataframe that contains for each file two extra columns: removed, and added containing 
    '''
  
    prev_schema = () # assume the initial schema is empty
    labels = ['year', 'month', 'removed', 'added']

    # Solution approach: 
    dataset = dataset.sort_values(by=['year', 'month'])
    removed = []  # list of columns removed
    added = []  # list of columns added


    for row in range(len(dataset)):
        a = dataset.iloc[row]['schema']
        b = dataset.iloc[row-1]['schema']

        if row != 0:
            rm = diff_schema(b,a)
            ad = diff_schema(a,b)
        else:
            rm = prev_schema
            ad = prev_schema

        removed.append(rm)
        added.append(ad)

    dataset['removed'] = removed
    dataset['added'] = added

    # convert the result list to the dataframe
    return pd.DataFrame(dataset, columns=labels)


names = ['year', 'month', 'schema', 'type']
df = pd.read_json('./data/dataset-description.json') 

# fhv_files
fhv_files = df[ df['type'] == 'fhv']
changes = analyze_schema_changes(fhv_files)
print(changes)

# fhvhv_files
fhvhv_files = df[ df['type'] == 'fhvhv']
changes = analyze_schema_changes(fhvhv_files)
#print(changes)

# yellow_files
yellow_files = df[ df['type'] == 'yellow']
changes = analyze_schema_changes(yellow_files)

# green_files
green_files = df[ df['type'] == 'green']
changes = analyze_schema_changes(green_files)




