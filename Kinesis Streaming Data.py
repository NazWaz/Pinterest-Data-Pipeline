# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
# URL processing
import urllib

# COMMAND ----------

# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

## CREATES PIN DATAFRAME

df_pin_data = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-12b287eedf6d-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# Display dataframe
display(df_pin_data)

# COMMAND ----------

df_pin = df_pin_data.selectExpr("CAST(data as STRING)")

pin_schema = StructType([
  StructField("category", StringType(), True),
  StructField("description", StringType(), True),
  StructField("downloaded", LongType(), True),
  StructField("follower_count", StringType(), True),
  StructField("image_src", StringType(), True),
  StructField("index", LongType(), True),
  StructField("is_image_or_video", StringType(), True),
  StructField("poster_name", StringType(), True),
  StructField("save_location", StringType(), True),
  StructField("tag_list", StringType(), True),
  StructField("title", StringType(), True),
  StructField("unique_id", StringType(), True)
])

df_pin = df_pin.select(from_json("data", pin_schema).alias("pin_data")).select("pin_data.*")

display(df_pin)


# COMMAND ----------

## CLEANS PIN DATAFRAME

# Replace empty and entries with no relevant data with null
cleaned_df_pin = df_pin.replace({" " : None, "No description available Story format" : None, "Untitled" : None, "User Info Error" : None, "Image src error." : None, "multi-video(story page format)" : "video", "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e" : None, "No Title Data Available" : None})

# Ensure every follower count is a number
cleaned_df_pin = cleaned_df_pin.withColumn("follower_count" ,regexp_replace ("follower_count", "M", "000000"))
cleaned_df_pin = cleaned_df_pin.withColumn("follower_count" ,regexp_replace ("follower_count", "k", "000"))

# Change data types to integer
cleaned_df_pin = cleaned_df_pin.withColumn("follower_count", cleaned_df_pin["follower_count"].cast("integer"))
cleaned_df_pin = cleaned_df_pin.withColumn("downloaded", cleaned_df_pin["downloaded"].cast("integer"))
cleaned_df_pin = cleaned_df_pin.withColumn("index", cleaned_df_pin["index"].cast("integer"))

# Clean save location to only have save location path
cleaned_df_pin = cleaned_df_pin.withColumn("save_location" ,regexp_replace ("save_location", "Local save in /", "/"))

# Rename index column to ind
cleaned_df_pin = cleaned_df_pin.withColumnRenamed("index", "ind")

# Reorder columns
cleaned_df_pin = cleaned_df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")


# COMMAND ----------

cleaned_df_pin.printSchema()

# COMMAND ----------

display(cleaned_df_pin)

# COMMAND ----------

## READS PIN DATA

df_pin_data = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-12b287eedf6d-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

## CREATES PIN DATAFRAME 

df_pin = df_pin_data.selectExpr("CAST(data as STRING)")

pin_schema = StructType([
  StructField("category", StringType(), True),
  StructField("description", StringType(), True),
  StructField("downloaded", LongType(), True),
  StructField("follower_count", StringType(), True),
  StructField("image_src", StringType(), True),
  StructField("index", LongType(), True),
  StructField("is_image_or_video", StringType(), True),
  StructField("poster_name", StringType(), True),
  StructField("save_location", StringType(), True),
  StructField("tag_list", StringType(), True),
  StructField("title", StringType(), True),
  StructField("unique_id", StringType(), True)
])

df_pin = df_pin.select(from_json("data", pin_schema).alias("pin_data")).select("pin_data.*")


## CLEANS PIN DATAFRAME

# Replace empty and entries with no relevant data with null
cleaned_df_pin = df_pin.replace({" " : None, "No description available Story format" : None, "Untitled" : None, "User Info Error" : None, "Image src error." : None, "multi-video(story page format)" : "video", "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e" : None, "No Title Data Available" : None})

# Ensure every follower count is a number
cleaned_df_pin = cleaned_df_pin.withColumn("follower_count" ,regexp_replace ("follower_count", "M", "000000"))
cleaned_df_pin = cleaned_df_pin.withColumn("follower_count" ,regexp_replace ("follower_count", "k", "000"))

# Change data types to integer
cleaned_df_pin = cleaned_df_pin.withColumn("follower_count", cleaned_df_pin["follower_count"].cast("integer"))
cleaned_df_pin = cleaned_df_pin.withColumn("downloaded", cleaned_df_pin["downloaded"].cast("integer"))
cleaned_df_pin = cleaned_df_pin.withColumn("index", cleaned_df_pin["index"].cast("integer"))

# Clean save location to only have save location path
cleaned_df_pin = cleaned_df_pin.withColumn("save_location" ,regexp_replace ("save_location", "Local save in /", "/"))

# Rename index column to ind
cleaned_df_pin = cleaned_df_pin.withColumnRenamed("index", "ind")

# Reorder columns
cleaned_df_pin = cleaned_df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")


## WRITES CLEANED PIN DATA

cleaned_df_pin.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("12b287eedf6d_pin_table")

# COMMAND ----------

# Deletes checkpoint folder
dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

# COMMAND ----------

## CREATES GEO DATAFRAME

df_geo_data = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-12b287eedf6d-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# Display dataframe
display(df_geo_data)

# COMMAND ----------

df_geo = df_geo_data.selectExpr("CAST(data as STRING)")

geo_schema = StructType([
  StructField("country", StringType(), True),
  StructField("ind", LongType(), True),
  StructField("latitude", DoubleType(), True),
  StructField("longitude", DoubleType(), True),
  StructField("timestamp", StringType(), True)
])

df_geo = df_geo.select(from_json("data", geo_schema).alias("geo_data")).select("geo_data.*")

display(df_geo)

# COMMAND ----------

## CLEANS GEO DATAFRAME

# Create coordinates column containing an array of latitude and longitude
cleaned_df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))

# Drop latitude and longitude columns
cleaned_df_geo = cleaned_df_geo.drop ("latitude", "longitude")

# Transform timestamp column data type to timestamp type
cleaned_df_geo = cleaned_df_geo.withColumn("timestamp", to_timestamp("timestamp"))

# Reorder columns
cleaned_df_geo = cleaned_df_geo.select("ind", "country", "coordinates", "timestamp")

display(cleaned_df_geo)

# COMMAND ----------

## READS GEO DATA

df_geo_data = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-12b287eedf6d-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

## CREATES GEO DATAFRAME

df_geo = df_geo_data.selectExpr("CAST(data as STRING)")

geo_schema = StructType([
  StructField("country", StringType(), True),
  StructField("ind", LongType(), True),
  StructField("latitude", DoubleType(), True),
  StructField("longitude", DoubleType(), True),
  StructField("timestamp", StringType(), True)
])

df_geo = df_geo.select(from_json("data", geo_schema).alias("geo_data")).select("geo_data.*")

## CLEANS GEO DATAFRAME

# Create coordinates column containing an array of latitude and longitude
cleaned_df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))

# Drop latitude and longitude columns
cleaned_df_geo = cleaned_df_geo.drop ("latitude", "longitude")

# Transform timestamp column data type to timestamp type
cleaned_df_geo = cleaned_df_geo.withColumn("timestamp", to_timestamp("timestamp"))

# Reorder columns
cleaned_df_geo = cleaned_df_geo.select("ind", "country", "coordinates", "timestamp")

## WRITES CLEANED GEO DATA

cleaned_df_geo.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("12b287eedf6d_geo_table")

# COMMAND ----------

## READS USER DATA

df_user_data = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-12b287eedf6d-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# Display dataframe
display(df_user_data)

# COMMAND ----------

df_user = df_user_data.selectExpr("CAST(data as STRING)")

user_schema = StructType([
  StructField("age", LongType(), True),
  StructField("date_joined", StringType(), True),
  StructField("first_name", StringType(), True),
  StructField("ind", LongType(), True),
  StructField("last_name", StringType(), True)
])

df_user = df_user.select(from_json("data", user_schema).alias("user_data")).select("user_data.*")

display(df_user)

# COMMAND ----------

## CLEANS USER DATAFRAME

# Create user_name column by concatenating first and last names
cleaned_df_user = df_user.withColumn("user_name", concat("first_name", "last_name"))

# Drop first and last name columns
cleaned_df_user = cleaned_df_user.drop("first_name", "last_name")

# Convert date_joined date type to timestamp
cleaned_df_user = cleaned_df_user.withColumn("date_joined", to_timestamp("date_joined"))

# Reorder columns
cleaned_df_user = cleaned_df_user.select("ind", "user_name", "age", "date_joined")

display(cleaned_df_user)

# COMMAND ----------

## READS USER DATA

df_user_data = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-12b287eedf6d-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

## CREATES USER DATAFRAME

df_user = df_user_data.selectExpr("CAST(data as STRING)")

user_schema = StructType([
  StructField("age", LongType(), True),
  StructField("date_joined", StringType(), True),
  StructField("first_name", StringType(), True),
  StructField("ind", LongType(), True),
  StructField("last_name", StringType(), True)
])

df_user = df_user.select(from_json("data", user_schema).alias("user_data")).select("user_data.*")

## CLEANS USER DATAFRAME

# Create user_name column by concatenating first and last names
cleaned_df_user = df_user.withColumn("user_name", concat("first_name", "last_name"))

# Drop first and last name columns
cleaned_df_user = cleaned_df_user.drop("first_name", "last_name")

# Convert date_joined date type to timestamp
cleaned_df_user = cleaned_df_user.withColumn("date_joined", to_timestamp("date_joined"))

# Reorder columns
cleaned_df_user = cleaned_df_user.select("ind", "user_name", "age", "date_joined")

## WRITES CLEANED USER DATA

cleaned_df_user.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("12b287eedf6d_user_table")

# COMMAND ----------


