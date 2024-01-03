# Pinterest Project

Pinterest project.

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








## Milestone 8 - Batch processing: AWS MWAA


![](Documentation/8/1.png)

![](Documentation/8/2.png)

![](Documentation/8/3.png)

![](Documentation/8/4.png)


## Milestone 9 - Stream processing: AWS Kinesis