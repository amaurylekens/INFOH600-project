import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pyspark import SparkFiles

def compute_location_id(long, lat):
    if not(long == 0 or long == "" or lat == 0 or lat == ""):
        # construct a rtree index with the geopanda df
        zones = gpd.read_file(SparkFiles.get("location.shp"))
        #zones = gpd.read_file("shape_files/location.shp")
        zones.set_geometry('geometry', crs=(u'epsg:'+str(4326)), inplace=True)
        rtree = zones.sindex

        # find possible match for the point
        pnt = Point(long, lat)
        possible_matches = list(rtree.intersection(pnt.bounds))

        # find the right zone
        for m in possible_matches:
            if zones.iloc[m].geometry.contains(pnt):
                return m+1