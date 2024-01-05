from pyspark.sql import types
# obs schema from e9
observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
])

station_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('lat', types.FloatType()),
    types.StructField('lon', types.FloatType()),
    types.StructField('elev', types.IntegerType()),
    types.StructField('state', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('gsn_flag', types.StringType()),
    types.StructField('hcn_flag', types.StringType()),
    types.StructField('wmo_id', types.StringType())
])

country_schema = types.StructType([
    types.StructField('abbrv', types.StringType()),
    types.StructField('country', types.StringType()),
])

city_schema = types.StructType([
    types.StructField('city', types.StringType()),
    types.StructField('city_lat', types.FloatType()),
    types.StructField('city_lon', types.FloatType()),
])

visits_schema = types.StructType([
    types.StructField('city', types.StringType()),
    types.StructField('country', types.StringType()),
    types.StructField('arrivals_2016_(mastercard)', types.FloatType()),
    types.StructField('arrivals_2018_(eurovision)', types.FloatType()),
    types.StructField('arrival_growth_2018(eurovision)', types.FloatType())
])