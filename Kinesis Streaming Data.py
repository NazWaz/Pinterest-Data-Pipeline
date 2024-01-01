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


