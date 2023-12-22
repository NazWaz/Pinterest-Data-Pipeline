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

![](Documentation/3/1.png)

- A key pair file was created locally in a linux directory `/home/nazwaz` as a `.pem` file in order to connect to the EC2 instance. The content of this key pair file was found by navigating to the parameter store and finding the specific key pair using the keypaird id 12b287eedf6d. By finding this, the value was copied including the `BEGIN` and `END` headers into the file and saved as `12b287eedf6d.pem`

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

- For the worker configuration, customised configuration was selected so the confluent-worker configuration set up earlier could be used and for the access permissions, the IAM role containing the user id `12b287eedf6d` was selected also. Once the connector had been created with the name `12b287eedf6d-connector`, it showed up as running in the connectors tab. 

## Milestone 5 - Batch processing: Configuring API in API gateway

## Milestone 6 - Batch processing: Databricks

## Milestone 7 - Batch processing: Spark on Databricks

## Milestone 8 - Batch processing: AWS MWAA

## Milestone 9 - Stream processing: AWS Kinesis