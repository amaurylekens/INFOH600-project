import pandas as pd 

# the parallelization is only needed in the reading of the metadata
# not in the computing of the statistics
df = pd.read_csv('/home/amaury/code/dataset-description.csv') 

f_types = ['yellow', 'green', 'fhv', 'fhvhv']

for f_type in f_types:
    select_data = df.loc[df['type'] == f_type][['size', 'num_records', 'type']]
    print('{} :'.format(f_type))
    print(select_data.describe())