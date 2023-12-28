# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
from pyspark.sql.window import Window
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
# dbutils.fs.unmount("/mnt/MOUNT")

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

display(cleaned_df_pin)
display(cleaned_df_geo)
display(cleaned_df_user)

# COMMAND ----------

# Combine pin and geo dataframes
popular_category_country_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_geo["ind"] == cleaned_df_pin["ind"], how="inner")

# Create window
window_spec = Window.partitionBy("country", "category")

# Create category count column
result_df = popular_category_country_df.withColumn("category_count", count("category").over(window_spec))

# Drop other columns
result_df = result_df.select("country", "category", "category_count")


window = Window.partitionBy("country", "category").orderBy("category_count")
new_df = result_df.withColumn("row", row_number().over(window))
new_df = new_df.filter(new_df.row == 1)


new_df = new_df.orderBy(["country", "category_count", "category"], ascending = [True, False, True])


new_df = new_df.drop("row")

display(popular_category_country_df)
display(result_df)
display(new_df)



# COMMAND ----------

## MOST POPULAR CATEGORY IN EACH COUNTRY

# Combine pin and geo dataframes
popular_category_country_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_geo["ind"] == cleaned_df_pin["ind"], how="inner")

# Create window
window = Window.partitionBy("country", "category")

# Create category count column and select columns to show
popular_category_country_df = popular_category_country_df.withColumn("category_count", count("category").over(window)).select("country", "category", "category_count")

# Add column assigning row numbers to each unique category within each country
popular_category_country_df = popular_category_country_df.withColumn("row", row_number().over(window.orderBy("category_count")))

# Filter rows so only rows with row number 1 remain and re order columns then drop row column
popular_category_country_df = popular_category_country_df.filter(popular_category_country_df.row == 1).orderBy(["country", "category_count", "category"], ascending = [True, False, True]).drop("row")

display(popular_category_country_df)

# COMMAND ----------

# Combine pin and geo dataframes
popular_category_year_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_geo["ind"] == cleaned_df_pin["ind"], how="inner")

result_df = popular_category_year_df.select("timestamp", "category")

dates = ("2018-01-01", "2022-12-31")

result_df = result_df.filter(result_df.timestamp.between(*dates))

result_df = result_df.select(year("timestamp").alias("post_year"), "category")

# Create window
window = Window.partitionBy("post_year", "category")

# Create category count column
result_df = result_df.withColumn("category_count", count("category").over(window))

#Create another window for row number
window = Window.partitionBy("post_year", "category").orderBy("category_count")

#
result_df = result_df.withColumn("row", row_number().over(window))

#
result_df = result_df.filter(result_df.row == 1).orderBy(["post_year", "category_count", "category"], ascending = [True, False, True]).drop("row")

display(result_df)


# COMMAND ----------

## MOST POPULAR CATEGORY EACH YEAR (2018 - 2022)

# Combine pin and geo dataframes
popular_category_year_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_geo["ind"] == cleaned_df_pin["ind"], how="inner")

# Range of dates from 2018 to 2022 
dates = ("2018-01-01", "2022-12-31")

# Filter out timestamp column for dates between 2018 and 2022, renaming it as post year with just the year value and select columns to show
popular_category_year_df = popular_category_year_df.filter(popular_category_year_df.timestamp.between(*dates)).select(year("timestamp").alias("post_year"), "category")

# Create window
window = Window.partitionBy("post_year", "category")

# Create category count column
popular_category_year_df = popular_category_year_df.withColumn("category_count", count("category").over(window))

# Add column assigning row numbers to each unique category for each year
popular_category_year_df = popular_category_year_df.withColumn("row", row_number().over(window.orderBy("category_count")))

# Filter rows so only rows with row number 1 remain and re order columns then drop row column
popular_category_year_df = popular_category_year_df.filter(popular_category_year_df.row == 1).orderBy(["post_year", "category_count", "category"], ascending = [True, False, True]).drop("row")

display(popular_category_year_df)

# COMMAND ----------

# Combine pin and geo datadrames
user_followers_country_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_geo["ind"] == cleaned_df_pin["ind"], how="inner")

# Select columns to show, drop null value rows and duplicate rows and change order
user_followers_country_df = user_followers_country_df.select("country", "poster_name", "follower_count").na.drop().dropDuplicates(["country", "poster_name"]).orderBy(["country", "follower_count"], ascending = [True, False])

# Group by country with the most followers for each country in descending order
country_most_followers_df = user_followers_country_df.groupBy("country").agg(max("follower_count").alias("follower_count")).orderBy("follower_count", ascending = False)

display(user_followers_country_df)
display(country_most_followers_df)

# COMMAND ----------

user_followers_country_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_geo["ind"] == cleaned_df_pin["ind"], how="inner")

