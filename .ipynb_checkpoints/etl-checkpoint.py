import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data_song, output_data):
    """Load JSON input data (song_data) from input_data path,
        process the data to extract song_table and artists_table, and
        store the queried data to parquet files.
    """
    print("Start processing song_data JSON files...")
    
    # Part 1: Load song_data
    # get filepath to song data file
    song_data = input_data_song

    # read song data file
    print("Reading song_data files from {}...".format(song_data))
    df_sd = spark.read.json(song_data)
    # printing the schema for reference
    print("Song_data schema:")
    df_sd.printSchema()
    
    
    # Part 2: Create and write to the songs_data table
    
    # creating a table view for the DF
    print("creating a song data table view")
    df_sd.createOrReplaceTempView("song_data")
   
    # extract columns to create songs table
    print("extracting columns and creating songs table")
    songs_table = spark.sql("""
    SELECT DISTINCT
        song_id, 
        title, 
        artist_id, 
        year, 
        duration 
    FROM song_data ORDER BY song_id  """) 
    
    print("Songs_table schema:")
    songs_table.printSchema()
    print("Songs_table examples:")
    songs_table.show(10, truncate=False)
    
    # Part 2.1: write the songs tables to files on S3
    # write songs to csv file on s3
    print("Writing artists csv file to s3")
    output_path = output_data + "sparkify_songs_table.csv"
    songs_table.write.save(output_path, format = 'csv', header = True)
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing songs partitioned parquet files to s3")
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data,"songs_partitioned"), "overwrite")
    
    # Part 3: Create and write to the artists_data table

    # extract columns to create artists table
    print("extracting columns and creating artists table")
    artists_table = spark.sql("""
        SELECT  artist_id        AS artist_id,
                artist_name      AS name,
                artist_location  AS location,
                artist_latitude  AS latitude,
                artist_longitude AS longitude
        FROM song_data
        ORDER BY artist_id desc
    """)
    
    # Part 3.1: write the artists tables to files on S3
    # write artists to csv file on s3
    
    print("Artists_table schema:")
    artists_table.printSchema()
    print("Artists_table examples:")
    artists_table.show(10, truncate=False)
    
    output_path = output_data + "sparkify_artists_table.csv"
    
    print("Writing artists csv file to s3")
    artists_table.write.save(output_path, format = 'csv', header = True)
    
    print("Writing artists partitioned parquet files to s3")
    # write artists table to parquet files partitioned by artist_id
    artists_table.write.partitionBy("artist_id").parquet(os.path.join(output_data,"artists_partitioned"), "overwrite")
    
    return songs_table, artists_tabl
    
def process_log_data(spark, input_data_song, input_data_log, output_data):
    """Load JSON input data (log_data) from input_data path,
        process the data to extract users_table, time_table,
        songplays_table, and store the queried data to parquet files.
    """
    # get filepath to log data file
    log_data = input_data_log

    # read log data file
    print("Reading log data jsons")
    df_ld = spark.read.json(log_data)
    
    # filter by actions for song plays
    print("filtering by actions for song plays")
    df_ld_filtered = df_ld.filter(df_ld.page == 'NextSong')

    # extract columns for users table
    print("extracting columns and creating users table")
    df_ld_filtered.createOrReplaceTempView("users_table_DF")
    users_table = spark.sql("""
        SELECT  DISTINCT userId    AS user_id,
                         firstName AS first_name,
                         lastName  AS last_name,
                         gender,
                         level
        FROM users_table_DF
        ORDER BY last_name
    """)
    print("Users_table schema:")
    users_table.printSchema()
    print("Users_table examples:")
    users_table.show(5)
    
    # write users table to parquet files
    print("Writing users partitioned parquet files to s3")
    users_table.write.partitionBy("user_id").parquet(os.path.join(output_data,"users_partitioned"), "overwrite")

    # create timestamp column from original timestamp column
 
    print("Creating timestamp column...")
    @udf(t.TimestampType())
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts / 1000.0)

    df_ld_filtered = df_ld_filtered.withColumn("timestamp", \
                        get_timestamp("ts"))
    
    df_ld_filtered.printSchema()
    
    df_ld_filtered.show(5)
    
    # create datetime column from original timestamp column
    print("Creating datetime column...")
    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0)\
                       .strftime('%Y-%m-%d %H:%M:%S')

    df_ld_filtered = df_ld_filtered.withColumn("datetime", \
                        get_datetime("ts"))
    print("Log_data + timestamp + datetime columns schema:")
    df_ld_filtered.printSchema()
    print("Log_data + timestamp + datetime columns examples:")
    df_ld_filtered.show(5)
    
    # extract columns to create time table
    print("extracting columns and creating time table")
    df_ld_filtered.createOrReplaceTempView("time_table_DF")
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
    print("Time_table schema:")
    time_table.printSchema()
    print("Time_table examples:")
    time_table.show(5)
    
    # write time table to parquet files partitioned by year and month
    print("Writing time table partitioned parquet files to s3")
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,"time_partitioned"), "overwrite")
    
    # read in song data to use for songplays table
    song_data = input_data_song
    
    print("Reading song_data files from {}...".format(song_data))
    df_sd = spark.read.json(song_data)

    # Join log_data and song_data DFs
    print("Joining log_data and song_data DFs...")
    df_ld_sd_joined = df_ld_filtered.join(df_sd, (df_ld_filtered.artist == df_sd.artist_name) & \
                     (df_ld_filtered.song == df_sd.title))
    print("...finished joining song_data and log_data DFs.")
    print("Joined song_data + log_data schema:")
    df_ld_sd_joined.printSchema()
    print("Joined song_data + log_data examples:")
    df_ld_sd_joined.show(5)

    # extract columns from joined song and log datasets
    # to create songplays table
    print("Extracting columns from joined DF...")
    df_ld_sd_joined = df_ld_sd_joined.withColumn("songplay_id", \
                        monotonically_increasing_id())
    
    df_ld_sd_joined.createOrReplaceTempView("songplays_table_DF")
    
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

    print("Songplays_table schema:")
    songplays_table.printSchema()
    print("Songplays_table examples:")
    songplays_table.show(5, truncate=False)

    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays table partitioned parquet files to s3")
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,"songplays_partitioned"), "overwrite")

def main():
    spark = create_spark_session()
    
    # full input data paths
    # input_data_song_full = "s3a://udacity-dend/song_data/*/*/*/*.json"
    # input_data_log_full = "s3a://udacity-dend/log_data/*/*/*.json"
    
    # sample input data paths
    input_data_song = "s3a://dend-spark-udacity/song_data/*/*/*/*.json"
    input_data_log = "s3a://dend-spark-udacity/log_data/*.json"
    output_data = "s3a://dend-spark-udacity/output_data/"
    
#     process_song_data(spark, input_data_song=input_data_song, output_data=output_data)    
    process_log_data(spark, input_data_song=input_data_song, input_data_log=input_data_log, output_data=output_data)

    print("Finished ETL Process!!!")

if __name__ == "__main__":
    main()
