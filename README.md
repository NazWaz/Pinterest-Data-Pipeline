# Pinterest Project

This project involved creating a data pipeline using the AWS cloud and Pinterest data. 

## Milestone 1 - Environment setup

The first milestone involved setting up the Github repository as well as the dev environment. For this project, pymysql was installed through pip and several modules were imported including requests, boto3 and sqlalchemy. The AWS account that was to be used for this project was also set up at this stage using pre-existing credentials provided - various aspects of the project were provided along with an IAM user and SSH Keypair ID in order to locate all the correct resources and to be able to access them.

![](Documentation/2/1.png)

## Milestone 2 - Starting to build the pipeline

The second milestone was to import a python file into VSCode containing a script that communicated with an RDS database containing data to receive it in three tables: pinterest_data, geolocation_data and user_data. The data output here when running this was to emulate what a Pinterest API would receive when a POST request is made by users uploading data to Pinterest.

![](Documentation/2/2.png)

- A class was created here with a method `create_db_connector()` with various parameters such as `HOST`, `USER`, `PASSWORD`, `DATABASE`, and `PORT` all of which allowed a connection to the database containing all the data we need for this project.

![](Documentation/2/3.png)

- The function `run_infinite_post_data_loop` was used to create an infinite post data loop using a `while True:` loop to continuously receive and output data. Various SQL commands are also used to specifically receive a single random row of data from each table of data.

- The three tables contained data about posts being updated to Pinterest (`pinterest_data`), data about the geolocation of each Pinterest post (`geolocation_data`) and data about the user that uploaded each post (`user_data`). `For` loops were used to iterate through each set of data in order to output them as a dictionary of key value pairs with the headings as the key and the data itself as the value using `dict(row._mapping)`.


## Milestone 3 - Batch processing: EC2 Kafka client configuration

The third milestone was to connect to an MSK cluster and set up Kafka on a client EC2 instance.

- A key pair file was created locally in a linux directory `/home/nazwaz` as a `.pem` file in order to connect to the EC2 instance. The content of this key pair file was found by navigating to the parameter store and finding the specific key pair. By finding this, the value was copied including the `BEGIN` and `END` headers into the file and saved as `12b287eedf6d.pem`

![](Documentation/3/2.png)

- To ensure the key was not publicly viewable, the following command was used: `chmod 400 12b287eedf6d-key-pair.pem`. Then another command was used in the WSL terminal to connect to the EC2 isntance using it's public DNS: 
`ssh -i "12b287eedf6d-key-pair.pem" root@ec2-54-81-124-13.compute-1.amazonaws.com`.

- Within the EC2 client, Java was installed using `sudo yum install java-1.8.0` and Apache Kafka was downloaded first using 
`wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz`. This zip file was then extracted using 
`tar -xzf kafka_2.12-2.8.1.tgz`. 

![](Documentation/3/3.png)

- Using `ls` in the home directory showed the kafka directory and within the `libs` directory here the command 
`wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.5/aws-msk-iam-auth-1.1.1-all.jar` was run to download the IAM MSK authentication package from github. This was neccessary as the MSK cluster used for this project uses IAM to check whether the client is authenticated and authorised to produce to the cluster.

![](Documentation/3/5.png)

![](Documentation/3/4.png)

- An environment variable called `CLASSPATH` was created to store the authentication jar file so that commands executed anywhere in the kafka client can be used. This was done using `export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.1-all.jar` within the bashrc file which could be edited using `nano ~/.bashrc`. It was added here so that the CLASSPATH was set up for every instance or new session. This was checked using `echo $CLASSPATH` to see if the path that is output is the same as the path assigned to CLASSPATH. 

![](Documentation/3/6.png)

- The IAM console on AWS was needed here to properly authenticate to the MSK cluster. The role tied to my user id was found and the role ARN here `arn:aws:iam::584739742957:role/12b287eedf6d-ec2-access-role` was needed. Within trust relationships, the trust policy was edited by adding a principal with 'IAM roles' was the principal type and replacing the ARN value with what was just copied.

![](Documentation/3/8.png)

![](Documentation/3/7.png)

- To finish the Kafka client configuration to use AWS IAM, a file was created in the `bin` folder using `nano client.properties`. Again, the ARN copied from earlier is also copied into this `client.properties` file.

![](Documentation/3/9.png)

- Before any Kafka topics could be created, 2 things were needed from the MSK cluster: the Bootstrap servers string `b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098` and the Plaintext Apache Zookeeper connection string `z-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:2181,z-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:2181,z-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:2181`. These were both found under client informatin through the MSK management console.

![](Documentation/3/10.png)

- Within the Kafka bin folder, the three topics `12b287eedf6d.pin`, `12b287eedf6d.geo` and `12b287eedf6d.user` were created using the following commands: 

`./kafka-topics.sh --bootstrap-server b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --command-config client.properties --create --topic 12b287eedf6d.pin`

`./kafka-topics.sh --bootstrap-server b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --command-config client.properties --create --topic 12b287eedf6d.geo`

`./kafka-topics.sh --bootstrap-server b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --command-config client.properties --create --topic 12b287eedf6d.user`

- `./kafka-topics.sh` was used to create the topic with several parameters passed through including the bootstrap server string, the client.properties file for permissions and the name of the topic.

## Milestone 4 - Batch processing: Connecting MSK cluster to S3 bucket

The fourth milestone was to connect the MSK cluster to an S3 bucket using MSK connect so that any data sent to the cluster would be automatically saved and stored within this S3 bucket. The bucket for this project had already been created with the name `user-12b287eedf6d-bucket` along with a VPC endpoint to S3 and an IAM role needed to write to this bucket.

![](Documentation/4/1.png)

- In order to create a custom plugin, containing code defining the logic of the connector created later on, the EC2 client was used again to connect to the cluster. First `sudo -u ec2-user -i` was used to assume admin priveleges and a directory for the connector was created using `mkdir kafka-connect-s3 && cd kafka-connect-s3`. Then the Confluent.io Amazon S3 connector was downloaded using `wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip` and then copied to the S3 bucket with `aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://user-12b287eedf6d-bucket/kafka-connect-s3/`.

![](Documentation/4/2.png)

- The `kafka-connect-s3` folder was now created here inside the S3 bucket with the zip file.

![](Documentation/4/3.png)

- Next, the custom plugin `12b287eedf6d-plugin` was created under the MSK connect section of the MSK console using the S3 object url and the zip file that was saved here. Now the connector could be created using this custom plugin.

![](Documentation/4/4.png)

- The connector was created using the MSK connect section of the MSK console again, selecting the pinterest MSK cluster along with specific configuration settings. In these settings the `topics.regex` field was given the value `12b287eedf6d.*` to ensure all the data going through the three topics was saved to the S3 bucket. 

![](Documentation/4/5.png)

- For the worker configuration, customised configuration was selected so the confluent-worker configuration set up earlier could be used and for the access permissions, the IAM role containing the user id `12b287eedf6d` was selected also. Once the connector had been created with the name `12b287eedf6d-connector`, it showed up as running in the connectors tab. Now any data sent from the MSK cluster to the S3 bucket is uploaded into a folder called `topics`.

## Milestone 5 - Batch processing: Configuring API in API gateway

The fifth milestone was to build an API to send data to the MSK cluster and store it in the S3 bucket. The API itself was already created and provided so the resources and methods needed to be added.

![](Documentation/5/1.png)

- Using the API gateway, the API with the name `12b287eedf6d` was located and a `{proxy+}` resource was created. This proxy resource with the `ANY` method allows the integration access to all available resources because of the greedy parameter `{proxy+}`

![](Documentation/5/2.png)

- A HTTP `ANY` method was created with the endpoint url `http://ec2-54-81-124-13.compute-1.amazonaws.com:8082/{proxy}` which used the public DNS from the EC2 instance previously created.

![](Documentation/5/3.png)

- Once the proxy resource and `ANY` method was added to the API, it was then deployed with a stage name `test`. The invoke url `https://5i08sjvi96.execute-api.us-east-1.amazonaws.com/test` was important when communicating with the API later on.

![](Documentation/5/4.png)

- In order to consume the data using MSK from the API, a REST proxy packaged needed to be installed on the EC2 client, to communicate with the MSK cluster. `sudo wget https://packages.confluent.io/archive/7.2/confluent-7.2.0.tar.gz` was used to download the package and `tar -xvzf confluent-7.2.0.tar.gz` extracted the zip file, creating the `confluent-7.2.0` directory.

![](Documentation/5/5.png)

- The `kafka-rest.properties` file within the directory `confluent-7.2.0/etc/kafka-rest` needed to be modified to perform IAM authentication. The correct Booststrap server and Plaintext Apache Zookeeper connection strings were added in here along with the IAM MSK authentication package to allow communication between the REST proxy and the cluster brokers.

![](Documentation/5/6.png)

- The REST proxy had to be started before sending any messages to the API to make sure they are consumed in MSK. This was done in the `confluent-7.2.0/bin` directory using the command `./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties`. Once the proxy is ready to receive requests, the message `INFO Server started, listening for requests` could be seen.

![](Documentation/5/7.png)

- The `user_posting_emulation.py` file was modified to send data to the Kafka topics using the previously acquired API invoke url. Here, the invoke url was customised to set the destination to `12b287eedf6d.pin` within the topics folder in the S3 bucket. `json.dumps` was used to serialise the data from the `pin` table into a json file with the structure defined and the key value pairs being the header names and the records under each heading. 

![](Documentation/5/8.png)

- The data from the `geo` table was instead stored in `12b287eedf6d.geo`, also within the topics folder. As the records within this table were different, each individual key value pair had to be identified to ensure the data was serialised correctly. `default=str` was also passed through here to ensure the timestamps were converted to strings before being converted into a json file.

![](Documentation/5/9.png)

- The data from the `user` table was stored in `12b287eedf6d.user` similarly to before.

![](Documentation/5/10.png)

- By printing the status code, the success of the requests could be monitored. A code of 200 indicated that the POST requests were successful.

![](Documentation/5/11.png)

- The REST proxy showed each request being sent to the API aswell as the destination for the data.

![](Documentation/5/12.png)

- Kafka consumers could also be created within the EC2 client to monitor the data being sent. Using `./kafka-console-consumer.sh --bootstrap-server b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --group students --consumer.config client.properties --topic 12b287eedf6d.pin --from-beginning ` a consumer for the `pin` data was made. The Bootstrap server was used here again, aswell as the flags `--group students`, `--from beginning` and `--confumer.config client.properties` to ensure all the data could be seen in the consumer and that the correct permissions were being used.

![](Documentation/5/13.png)

- A consumer for the `geo` data was made using `./kafka-console-consumer.sh --bootstrap-server b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --group students --consumer.config client.properties --topic 12b287eedf6d.geo --from-beginning`.

![](Documentation/5/14.png)

- A consumer for the `user` data was made using `./kafka-console-consumer.sh --bootstrap-server b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --group students --consumer.config client.properties --topic 12b287eedf6d.user --from-beginning`

![](Documentation/5/15.png)

- The data could also be checked and monitored within the S3 bucket. A topics folder was now present containing folders for all three topics.

![](Documentation/5/16.png)

- Within each topic folder was a partition containing all the data records as json files.

## Milestone 6 - Batch processing: Databricks

The sixth milestone was to set up Databricks and mount the S3 bucket to Databricks. The AWS access and secret access keys did not need to be created at this stage as the account being used had already been granted all the neccessary permissions and full access, with the credentials uploaded into a csv file.

![](Documentation/6/1.png)

![](Documentation/6/2.png)

- The libraries for pyspark operations were imported and the csv file `authentication_credentials.csv` containing the AWS keys was read to Databricks into a spark dataframe using `spark.read()`.

![](Documentation/6/3.png)

- The access and secret access keys were extracted from the spark dataframe and every character of the secret access key was encoded even using `urllib.parse.quote()` for added security. 

![](Documentation/6/4.png)

- Finally the S3 bucket was mounted using the S3 URL and the mount name `MOUNT` using `dbutils.fs.mount()`.

![](Documentation/6/5.png)

![](Documentation/6/6.png)

![](Documentation/6/7.png)

- The three dataframes `df_pin`, `df_geo` and `df_user` could then be created using the jsons from the S3 bucket. The file path to these json objects is given as the path added on to the mount path and again using `spark.read()`, each dataframe is created. There is no custom schema i.e. Spark infers the structure of the dataframe when mapping the json files into the dataframes.

## Milestone 7 - Batch processing: Spark on Databricks

The seventh milestone was to perform several data cleaning operations and computations on each of the Databricks dataframes using Spark.

![](Documentation/7/1.png)

- First the `df_pin` dataframe containing the information about Pinterest posts was cleaned. As there were many entries with either no or irrelevant data, they were all replaced with null values using `.replace()` with a dictionary of key value pairs. Each key referred to what was going to be replaced within all columns of the dataframe and the value was `None` which would return `null`.

- The `follower_count` column contained letters such as k and M to represent thousand and million so `.withcolumn()` was used to replace this column along with `regexp_replace()` to find these characters and replace them with zeroes.

- The `follower_count` column was still a `string` data type so using `.withColumn()` together with `.cast()` the data type was changed to `integer`.

- The path in the `save_location` column was cleaned using `regexp_replace()` again to only include the save location path.

- The `index` column was renamed to `ind` using `.withColumNRenamed()`.

- Finally, all of the columns were reordered using `.select()` with the column names listed in order.

![](Documentation/7/2.png)

- Next the `df_geo` dataframe containing the information about geolocation was cleaned. 

- A new column `coordinates` was created using `.withColumn()` along with `array()` to combine the `latitude` and `longitude` columns into an array.

- These `latitude` and `longitude` columns were then dropped using `.drop()`.

- The `timestamp` column was also still a `string` data tpye so it was changed to a `timestamp` type using `.withColumn()` together with `to_timestamp()`.

- Finally, the columns were reordered.

![](Documentation/7/3.png)

- Lastly the `df_user` dataframe containing the information about users was cleaned.

- A new column `user_name` was created using `.withColumn` and `concat()` to concatenate the records from both `first_name` and `last_name` columns into one.

- These `first_name` and `last_name` columsn were then dropped.

- The `date_joined` column also needed the data type changed from `string` to `timestamp`.

- Finally, the columns were reordered.

![](Documentation/7/4.png)

![](Documentation/7/13.png)

- This query was to find the most popular category in each country.

- The `pin` and `geo` dataframes were combined using an `inner` join based on the common column `ind`.

- A window was created on this new dataframe using the `Window` class to specify columns to partition and order by. Here, the window was partitioned by the `country` column first using `.partitionBy` and then by the `category` column.

- The `count().over()` function was applied on this window using `.withColumn` to create a new column called `category_count` which contained the number of elements in each group i.e. the total number of categories for each country. The columns were then filtered again using `.select()`

- Another column `row` was added to assign a row number to each unique category within each country (window) using `.withColumn` and `row_number().over()` but this time the window was ordered by the `category_count` column. 

- The duplicate `category_count` rows could be eliminated now by only keeping the records which had a `row` number equal to 1 using `.filter`. The data was reordered using `.orderBy` and finally the `row` column was removed.

![](Documentation/7/5.png)

![](Documentation/7/14.png)

- This query was to find the most popular category each year.

- The `pin` and `geo` dataframes were combined using an `inner` join based on the common column `ind`.

- The `dates` variable was created and assigned dates between the started of 2018 and the end of 2022 as this was the range of dates to filter data between. Using `.between` and `.select(year())`, the dates were selected from the `timestamp column` and only the value of the year was taken. This column was named `post_year` using `.alias()`.

- A window was created and partitioned by `post_year` and `category`. 

- The `category_count()` column was created again using the window and `row_number()` functions.

![](Documentation/7/6.png)

![](Documentation/7/15.png)

- This query was to find the most popular user in each country and furthermore the most popular country.

- The `pin` and `user` dataframes were combined using an `inner` join based on the common column `ind`.

- The `country`, `poster_name` and `follower_count` columns were selected and any null or duplicate values were dropped using `.na.drop().dropDuplicates` in the `country` and `poster_name` columns. 

- The dataframe was reordered using `.groupBy` to order the rows using the `country` column before finding the rows with the highest `follower_count` for each country using `.agg(max())`. Then it was reordered by `follower_count` starting with the most popular countries.

![](Documentation/7/7.png)

![](Documentation/7/16.png)

- This query was to find the most popular category for different age groups. 

- The `pin` and `geo` dataframes were combined using an `inner` join based on the common column `ind`.

- The `age_group` column was created using `.withColumn()` and `.when()` and `.otherwise` conditional clauses to define what the name of the of row should be if the value is within a certain range i.e. if the `age` value was less than or equal to 24, it would be replaced with "18-24".

- A window was created and partitioned by `age_group` and `category`.

- The `category_count` column was created again using the window and `row_number()` functions. 

![](Documentation/7/8.png)

![](Documentation/7/17.png)

- This query was to find the median follower count for different age groups.

- The `pin` and `user` dataframes were combined using an `inner` join based on the common column `ind`.

- The `age_group` column was created again.

- The median of the `follower_count` column was found by grouping the dataframe by `age_group` and using `.agg(expr())` together with the expression `"percentile_approx()"` to create a column naming it `median_follower_count`. 

![](Documentation/7/9.png)

![](Documentation/7/18.png)

- This query was to find the number of users that joined each year.

- The `dates` variable was used to create a range of dates between 2015 and 2020 in order to find how many users joined each year during this time period.

- Only the year was taken from the `date_joined` column and created into a new column called `post_year`.

- A window was created and partitioned by `post_year`.

- The `number_users_joined` column was created using the window and `row_number()` functions.

![](Documentation/7/10.png)

![](Documentation/7/19.png)

- This query was to find the median follower count of users based on their joining year.

- The `pin` and `user` dataframes were combined using an `inner` join based on the common column `ind`.

- The `dates` variable was created to create the `post_year` column. After grouping the dataframe by `post_year`, the median could be found using `.agg(expr())` again to return the median values within the column `median_follower_count`. 

![](Documentation/7/11.png)

![](Documentation/7/20.png)

- This query was to find the median follower count of users based on both their joining year and age group.

- The `pin` and `user` dataframes were combined using an `inner` join based on the common column `ind`.

- The `age_group` column was created using `.withColumn()` together with `.when()` and `.otherwise()` similarly to before.

- This time, before finding the median, the dataframe was grouped by the `age_group` first and then the `post_year` to create the `median_follower_count` column.

![](Documentation/7/12.png)

- To unmount the bucket `dbutils.ds.unmount()` was used. This command was neccessary at the end every time because later the whole workbook would be run automatically and the bucket wouldnt be able to mount without being unmounted at the end each time.

## Milestone 8 - Batch processing: AWS MWAA

The eigth milestone was to use AWS MWAA (Managed Workflows for Apache Airflow) for an airflow environment to then create a DAG (Directed Acyclic Graph) to run the Pinterest Databricks notebook made earlier. The AWS provided already had access to a MWAA environment and to a dedicated S3 bucket `mwaa-dags-bucket` so creating the Databricks API token to connect MWAA and Databricks and create the `requirements.txt` file was not needed.

![](Documentation/8/1.png)

- The airflow DAG was made in VSCode using python. Various parameters were passed through to ensure it runs on a specific schedule as well as how often it should run/retry if the process failed. The `notebook_path` was given as the path to the Databricks notebook which was to be run from start to finish. The DAG was also named as `12b287eedf6d_dag`.

![](Documentation/8/4.png)

- This DAG was then uploaded to the designated S3 bucket so that it could then be accessed within the airflow UI.

![](Documentation/8/3.png)

![](Documentation/8/2.png)

- The DAG was triggered manually in order to check that it ran successfully which it did after an intial failure.

## Milestone 9 - Stream processing: AWS Kinesis

The ninth milestone was to send the Pinterest data instead to AWS Kinesis as data streams and then read and save this data within Databricks while it was streaming instead. 

![](Documentation/9/1.png)

- Three streams were set up using AWS Kinesis and called `streaming-12b287eedf6d-pin`, `streaming-12b287eedf6d-geo` and `streaming-12b287eedf6d-user`. 

![](Documentation/9/2.png)

- The REST API from earlier was configured to allow for Kinesis actions such as listing, creating, describing, deleting streams as well as adding records to streams by creating new resources and methods. The `streams` resource was created with the `GET` method to perform the Kinesis `ListStreams` action.

![](Documentation/9/3.png)

- Several settings were customised within the `integration type` tab including the aws region to be `us-east-1`, the AWS service as `Kinesis`, the HTTP method as `POST`, the action type as `User action name`, the action type as `ListStreams` and the execution role as `arn:aws:iam::584739742957:role/12b287eedf6d-kinesis-access-role`. This execution role was the ARN of the Kinesis Access Role and was used for every method from here on.

![](Documentation/9/4.png)

- Within the `integration request` tab, a header `application/x-amz-json-1.1` was added with the header type `Content-Type`. A  mapping template `{}` was added with the content type as `application/json`. 

![](Documentation/9/5.png)

- Under the `streams` resource a new child resource `{stream-name}` was added with a `GET`, `POST` and `DELETE` method.

![](Documentation/9/6.png)

- The `GET` method was created the same as before except this time with the action type as `DescribeStream` and a different mapping template. This template takes an input from an API request and outputs the `StreamName` based on the information provided in the request, which is expected to contain the stream name. `$input` represents the input payload received by the API while `params()` finds the value of the `stream-name` parameter.

![](Documentation/9/7.png)

- The `POST` method was created with the action type as `CreateStream` and a different mapping template. This template checks if the `ShardCount` field is empty within the input payload using a conditional and if it is it sets it to 5. If not it retrieves and outputs the value of `ShardCount` to what was received and the `StreamName` is found like before.

![](Documentation/9/8.png)

- The `DELETE` method was created with the action type as `DeleteStream` and had the same mapping template as the `GET` method earlier to find the `StreamName`.

![](Documentation/9/9.png)

- Two more child resources were added under `{stream-name}` called `record` and `records` and a `PUT` method was created for each resource. The first `PUT` method was created with the action type as `PutRecord` and a different mapping template. This template transforms the input payload sent to the API into the neccessary format to write a record to the AWS Kinesis streams. The `StreamName` is found like before. The `Data` field takes the `Data` value from the input and encodes it in a Base64 format using `$util.base64Encode()` because records could only be written to a Kinsesis stream in this format. Finally, the `PartitionKey` field takes the input value of the `PartitionKey`.

![](Documentation/9/10.png)

- The second `PUT` method was created with the action type as `PutRecords` with a different mapping template. This template instead allows for writing multiple records into the AWS Kinesis streams. An array called `Records` is used this time where a `for` loop iterates over each `record` within the array, encoding the `Data` and finding the `PartitionKey`. A conditional statement is added at the end of the loop to add a comma after each record except the final record to keep the keep the output JSON formatted correctly.

![](Documentation/9/11.png)

![](Documentation/9/12.png)
- Once the API had been fully updated, it was deployed again as a stage `test` and data needed to be sent now to the API. The previous script to send data to the MSK cluster was updated to instead send data using the API to the three AWS Kinesis streams.

![](Documentation/9/13.png)

- The invoke url was updated with a new path this time to the `record` resource, because this script was designed to add one record at a time to each stream. This meant that the type of request being sent had to be a `PUT` request instead. The structure of the `pin_data` had to be altered to send JSON messages in a way that can be recognised by the API, according to the template made within the `record` resource. The `StreamName` was specified as `streaming-12b287eedf6d-pin` aswell as the `Content-Type` as `application/json`.

![](Documentation/9/14.png)

- The `geo_data` structure was similar with the `StreamName` as `streaming-12b287eedf6d-geo` and different invoke url instead.

![](Documentation/9/15.png)

- The `user_data` structure was also similar with the `StreamName` as `streaming-12b287eedf6d-user` and different invoke url instead. 

![](Documentation/9/16.png)

- Once the script was run, a response code of 200 was shown to indicate that data was being sent succefully to the streams. Each record of data could be checked within the Kinesis Console. This was done by selecting the shard `shardId-000000000000`, the first shard as data is normally stored here. For the starting position, `At timestamp` was selected giving an approximate time of when the script was run. This then provided a list of all of the records sent to the stream as the data is being sent.

![](Documentation/9/17.png)

![](Documentation/9/20.png)

- Now that data was being injested into the AWS Kinesis data streams, it was ready to be read into Databricks. The ccommand `spark.readStream` was used with several arguments passed through including the `format` as `kinesis`, the `streamName`, the `intialPosition`, the `region` and again the `awsAccessKey` and `awsSecretKey` which were extracted like before. This command ran continously to return a dataframe with the columns `partitionKey`, `data`, `stream`, `shardId`, `sequenceNumber` and `approximateArrivalTimestamp`. 

- The column containing all the data needed to be deserialised by using `.selectExpr()` with the expression `"CAST(data as STRING)"` to return a dataframe of dictionaries for each record in each row. In order to arrange the data found in each row into their respective columns, a custom schema needed to be made using the `StructType()` function with each `StructField()` being used to indicate each column along with the allocated data type.

- Using this schema, `.select()` was used with `from_json()` along with the column `data` and then every column from here was selected again using `.select` for the `df_pin` dataframe. Now the data was in the correct structure and could be cleaned again.

![](Documentation/9/18.png)
 
![](Documentation/9/19.png)

- Once the data had been cleaned, it could now be written to a Databricks delta table. All of these operations, starting from reading the streaming data to writing the streaming data needed to be ran within the same cell. The command `.writeStream` was used with several arguments passed through including the `format` as `delta`, the `outputMode` as `append`, the `checkpointLocation` as `/tmp/kinesis/_checkpoints/` and the table name `12b287eedf6d_pin_table` using `.table()`. 

- The `append` output mode was used so that only new rows were appended to each result table were written to external storage since the last trigger. The `checkpointLocation` procided a checkpoint which is typical with streaming queries for fault tolerance and ensured resiliency. 

![](Documentation/9/21.png)

- The geo data was read similarly except this time with a different schema to match the structure of the data. 

![](Documentation/9/22.png)

![](Documentation/9/23.png)

- The geo data was written similarly to instead this time to a table called `12b287eedf6d_geo_table`

![](Documentation/9/24.png)

- The user data was read similarly except this time with a different schema to match the structure of the data. 

![](Documentation/9/25.png)

![](Documentation/9/26.png)

- The user data was written similarly to instead this time to a table called `12b287eedf6d_user_table`

![](Documentation/9/27.png)

- Before the `writeStream` function could be used again the checkpoint folder needed to be deleted each time using `dbutils.fs.rm()`.

![](Documentation/9/28.png)

![](Documentation/9/29.png)

![](Documentation/9/30.png)

- The `readStream` and `writeStream` queries would run indefinitely so after interrupting them, the delta tables could be checked within the catalog. For each table, the data was saved successfully as expected.