# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
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

# unmount bucket
dbutils.fs.unmount("/mnt/MOUNT")

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "user-12b287eedf6d-bucket"
# Mount name for the bucket
MOUNT = "/mnt/MOUNT"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/MOUNT/topics"))

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/MOUNT/topics/12b287eedf6d.pin/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_pin)

# COMMAND ----------

cleaned_df_pin.printSchema()

# COMMAND ----------

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

display(cleaned_df_pin)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/MOUNT/topics/12b287eedf6d.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_geo)

# COMMAND ----------

# Create coordinates column containing an array of latitude and longitude
cleaned_df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))

# Drop latitude and longitude columns
cleaned_df_geo = cleaned_df_geo.drop ("latitude", "longitude")

# Transform timestamp column data type to timestamp type
cleaned_df_geo = cleaned_df_geo.withColumn("timestamp", to_timestamp("timestamp"))

# Reorder columns
cleaned_df_geo = cleaned_df_geo.select("ind", "country", "coordinates", "timestamp")

# COMMAND ----------

cleaned_df_geo.printSchema()

# COMMAND ----------

display(cleaned_df_geo)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/MOUNT/topics/12b287eedf6d.user/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_user)

# COMMAND ----------

# Create user_name column by concatenating first and last names
cleaned_df_user = df_user.withColumn("user_name", concat("first_name", "last_name"))

# Drop first and last name columns
cleaned_df_user = cleaned_df_user.drop("first_name", "last_name")

# Convert date_joined date type to timestamp
cleaned_df_user = cleaned_df_user.withColumn("date_joined", to_timestamp("date_joined"))

# Reorder columns
cleaned_df_user = cleaned_df_user.select("ind", "user_name", "age", "date_joined")

# COMMAND ----------

cleaned_df_user.printSchema()

# COMMAND ----------

display(cleaned_df_user)

# COMMAND ----------


