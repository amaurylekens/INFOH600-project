import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

def compute_location_id(long, lat):
     
    zones = gpd.read_file('./location.shp')

    pnt = Point(long, lat)
    
    df = [[lid, pnt.within(geom)] 
          for lid, geom in zip(zones['location_i'],
                               zones['geometry'])]

    df = pd.DataFrame(df, columns = ['location_id' ,'is_in']) 
    
    
    location = df.loc[df['is_in'] == True]['location_id']

    if location.shape[0] == 1:
        return(int(location))

    else:
        return -1
