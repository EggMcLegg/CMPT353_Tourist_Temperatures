import pandas as pd
from pathlib import Path

def convert_stations():
    # https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
    # Define the column names for the CSV file.
    columns = ['station_id', 'latitude', 'longitude', 'elevation', 'state', 'name', 'gsn_flag', 'hcn_flag', 'wmo_id']
    # read_fwf usage from here: https://stackoverflow.com/questions/21546739/load-data-from-txt-with-pandas
    data = pd.read_fwf(Path("raw_data/ghcnd-stations.txt"), header = None, names = columns, dtype = str)

    data.to_csv(Path("processed_data/ghcnd-stations.csv"), index = False)

def convert_countries():
    # https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt
    columns = ['abbrv', 'country']
    data = pd.read_fwf(Path("raw_data/ghcnd-countries.txt"), header = None, names = columns, dtype = str)

    data.to_csv(Path("processed_data/ghcnd-countries.csv"), index = False)

def convert_cities():
    # geonames https://download.geonames.org/export/dump/cities15000.zip
    # used as a guide for the dataset: https://download.geonames.org/export/dump/readme.txt
    columns = ['name', 'lat', 'lon']
    indices = [1, 4, 5]
    data = pd.read_csv(Path("raw_data/cities15000.txt"), sep = "\t", header = None, usecols = indices, names = columns, dtype = str)

    data['lat'] = data['lat'].astype(float).round(4)
    data['lon'] = data['lon'].astype(float).round(4)
    
    data.to_csv(Path("processed_data/city-coords.csv"), index = False)