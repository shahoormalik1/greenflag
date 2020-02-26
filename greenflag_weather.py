import pandas as pd
import pyarrow

# Use pandas to read csv and combine weather data for both months

df_feb = pd.read_csv(r'C:\Users\shaho\Downloads\Data Engineer Test\weather.20160201.csv')
df_mar = pd.read_csv(r'C:\Users\shaho\Downloads\Data Engineer Test\weather.20160301.csv')

df_all = pd.concat([df_feb, df_mar], ignore_index=True)

df_all.to_parquet('weather_data.parquet')

from pyspark.sql import SparkSession

# initialise sparkContext

spark = SparkSession.builder \
    .master('local') \
    .appName('GF_Test') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()

sc = spark.sparkContext

# using SQLContext to read parquet file

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# to read parquet file

df = sqlContext.read.parquet('weather_data.parquet')

df.createOrReplaceTempView("weather")


spark.sql('select ObservationDate, ScreenTemperature, Region from weather order by ScreenTemperature desc limit 1 ').show()