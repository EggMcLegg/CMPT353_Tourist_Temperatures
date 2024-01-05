import sys

from pyspark.sql import SparkSession, functions, Row, types
from pyspark.sql.functions import col
import numpy as np

# # geopy stuff from
# # https://geopy.readthedocs.io/en/stable/index.html#nominatim
# # and
# # https://www.geeksforgeeks.org/get-the-city-state-and-country-names-from-latitude-and-longitude-using-python/
# from geopy.geocoders import Nominatim
# from geopy.extra.rate_limiter import RateLimiter

import wikipedia_scraper
import txt_converter
import schemas

# # kept getting errors
# # https://geopy.readthedocs.io/en/stable/index.html?highlight=error#usage-with-pandas
# geolocator = Nominatim(user_agent="process GHCN data")

# def get_city(lat, lon):
#     location = geolocator.reverse(lat+","+lon)

#     if location != None and 'city' in location.raw['address']:
#         return location.raw["address"].get("city", "")
#     else: 
#         return "Unknown"


def main(in_dir):
    
    weather = spark.read.csv(in_dir, schema = schemas.observation_schema)
    weather = weather.cache()

    # load in station & country data for state/country
    stations = spark.read.csv("processed_data/ghcnd-stations.csv", sep = ",", schema = schemas.station_schema)
    countries = spark.read.csv("processed_data/ghcnd-countries.csv", sep = ",", schema = schemas.country_schema)

    # clean weather a bit (same as e8)
    weather = weather.filter(weather.qflag.isNull())    
    # join station data to label the data
    labelled_data = weather.join(stations, "station", how = "inner")
    labelled_data = labelled_data.cache()
    # join country to df (could probably also use geopy to do this)
    matching_country = (functions.col("abbrv") == functions.col("station").substr(1, 2))
    labelled_data = labelled_data.join(countries, matching_country, "left_outer")

    countries.drop()
    weather.drop()

    cities = spark.read.csv("processed_data/city-coords.csv", sep = ",", schema = schemas.city_schema)
    
    labelled_data = labelled_data.join(cities)
    #labelled_data.write.csv("city_output", mode = "overwrite", header = True)
    
    #labelled_data = labelled_data.drop("city")
    labelled_data = labelled_data.withColumn("abs_lat", functions.abs(functions.expr("lat")))
    labelled_data = labelled_data.withColumn("abs_lon", functions.abs(functions.expr("lon")))
    labelled_data = labelled_data.withColumn("abs_city_lat", functions.abs(functions.expr("city_lat")))
    labelled_data = labelled_data.withColumn("abs_city_lon", functions.abs(functions.expr("city_lon")))

    labelled_data = labelled_data.withColumn("lat_diff", functions.abs(functions.expr("abs_lat - abs_city_lat")))
    labelled_data = labelled_data.withColumn("lon_diff", functions.abs(functions.expr("abs_lon - abs_city_lon")))
    
    visits = spark.read.csv("processed_data/city_counts.csv", sep = ",", schema = schemas.visits_schema)
    
    # within ~1km
    labelled_data = labelled_data.filter((labelled_data["lat_diff"] < 0.35) & (labelled_data["lon_diff"] < 0.35))
    
    popular_data = labelled_data.drop("country")

    popular_data = popular_data.join(visits, "city", how = "inner")

    # drop duplicates

    # select the useful columns
    popular_data = popular_data.select("country", "city", "station", "date", "observation", "value", "arrivals_2016_(mastercard)", "arrivals_2018_(eurovision)")
    
    popular_data.write.csv("popular_labelled_output", mode = "overwrite", header = True)

    labelled_data = labelled_data.select("country", "station", "date", "observation", "value")
    labelled_data.write.csv("all_labelled_output", mode = "overwrite", header = True)

if __name__ == "__main__":
    in_directory = sys.argv[1]

    wikipedia_scraper.scrape()
    txt_converter.convert_stations()
    txt_converter.convert_countries()
    txt_converter.convert_cities()

    # from e11
    spark = SparkSession.builder.appName('Process GHCN Data').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory)