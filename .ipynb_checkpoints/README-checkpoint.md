## Introduction

The goal of this project is to create an ETl pipeline for the fake music streaming company Sparkify. This pipeline will extract music data from an AWS S3 bucket, proccess it using Spark, and load the data back into S3 as a set of dimensional tables for their analytics team to continue finding insights regarding what songs their users are listening to. We'll test our database and ETL pipeline by running queries given by the "Sparkify analytics team" and compare our results with their expected results.

##### Steps To Complete:
1) Create a star schema optimized for queries on song play analysis
2) Load data from the S3 bucket into spark data frames
3) Execute Spark SQL statements that create the analytics tables
4) Write them back to S3 bucket in parquet format

Parquet is a columnar format that is supported by many other data processing systems. Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema of the original data. When writing Parquet files, all columns are automatically converted to be nullable for compatibility reasons.

###### Files & Links Used:

| File | Description |
| ------ | ------ |
| song_data | Song JSON data in S3 bucket 'udacity-dend'|
| log_data | Log JSON data in S3 bucket 'udacity-dend'|
| dwh.cfg | Configuration file for AWS resource access |
| sql_queries.py | Python script create, copy and insert SQL tables  |
| dl.cfg | File containing your AWS credentials|
| etl.py | Python script reads data from S3, processes that data using Spark, and writes them back to S3  |

### Amazon Web Services

To create the cluster with your credentials place your AWS key and secret into dwh.cfg in the following structure:
```
ADD YOUR CENDENTIALS:
AWS_KEY=<your aws key>
AWS_SECRET=<your aws secret>
```
These were the source s3 locations of the data used in this implementation:
```
SOURCE FILES:
Song data: s3a://udacity-dend/song_data
Log data: s3a://udacity-dend/log_data
```
Since the total data in these source folders was very large, I used a small subset of the data to run the ETL process. This data is in the sample_data folder.
This was copied into an s3 bucket. To recreate this, simply upload the files to your own s3 bucket.

After dropping the files in your s3 bucket, these were the paths used for getting input data and writing output data:
```
Input_data (song) = s3a://<name_of_your_bucket>/song_data
Input_data (log) = s3a://<name_of_your_bucket>/log_data
Output_data = s3a://<name_of_your_bucket>/output/
```


### Running the ETL Process

Running the ETL process is divided in three parts:
1. Load raw files from the AWS resources
2. Use Spark SQL to create tables
3. Write back tables into cloud storage

To run the script, make sure all dependencies are installed in your environment, then navigate to the project directory and run:
```
python etl.py
```

For our data warehouse, one fact table and four dimension tables need to be created.

#### Fact Table: songplays
```
songplays_table = spark.sql("""
        SELECT  songplay_id AS songplay_id,
                month(timestamp) as month,
                year(timestamp) as year,
                timestamp   AS start_time,
                userId      AS user_id,
                level       AS level,
                song_id     AS song_id,
                artist_id   AS artist_id,
                sessionId   AS session_id,
                location    AS location,
                userAgent   AS user_agent
        FROM songplays_table_DF
        ORDER BY (user_id, session_id)
    """)

```

#### Dimension Table: Users
```
users_table = spark.sql("""
        SELECT  DISTINCT userId    AS user_id,
                         firstName AS first_name,
                         lastName  AS last_name,
                         gender,
                         level
        FROM users_table_DF
        ORDER BY last_name """)
```

##### Dimension Table: Songs
```
songs_table = spark.sql("""
    SELECT DISTINCT
        song_id, 
        title, 
        artist_id, 
        year, 
        duration 
    FROM song_data ORDER BY song_id  """) 
```
##### Dimension Table: Artists
```
artists_table = spark.sql("""
        SELECT  artist_id        AS artist_id,
                artist_name      AS name,
                artist_location  AS location,
                artist_latitude  AS latitude,
                artist_longitude AS longitude
        FROM song_data
        ORDER BY artist_id desc """)
    
artists_table.write.partitionBy("artist_id").parquet(os.path.join(output_data,"artists"), "overwrite")    
```
##### Dimension Table: Time
```
time_table = spark.sql("""
        SELECT  DISTINCT datetime AS start_time,
                         hour(timestamp) AS hour,
                         day(timestamp)  AS day,
                         weekofyear(timestamp) AS week,
                         month(timestamp) AS month,
                         year(timestamp) AS year,
                         dayofweek(timestamp) AS weekday
        FROM time_table_DF
        ORDER BY start_time
    """)
    
time_table.write.partitionBy("timestamp").parquet(os.path.join(output_data,"time"), "overwrite") 