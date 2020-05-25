import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''Creates a spark seassion'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Ingests songs data from song_data and extracts columns of songs and artists
    tables. 
    Result is written in parquet format and pushed back to S3. 
    
    Parameters: Spark session, 
    input_data = song_data where the songs sits on S3. 
    output_data = Path where the parquet files will be written to
    '''
    # setting the filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # reading song data file
    df = spark.read.json(song_data)

    # extracting columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 
                            'year', 'duration')\
                    .dropDuplicates()
    #creatinging a view 
    songs_table.CreateorReplaceTempView('songs')
    
    # writing songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id')\
                    .parquet(os.path.join(output_data, 'songs/songs.parquet'),'overwrite')

    # extracting columns to create artists table
    artists_table = df.select('artist_id', 
                              'artist_name',
                              'artist_location',
                              'artist_latitude',
                            'artist_longitude')\
                        .withColumnRenamed('artist_name', 'name')\
                        .withColumnRenamed('artist_location', 'location')\
                        .withColumnRenamed('artist_latitude', 'latittude')\
                        .withColumnRenamed('artist_longitude', 'longitude')\
                        .dropDruplicates()
    
     #creating a view 
    artists_table.CreateorReplaceTempView('artist')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    Ingests logs data from log_data and extracts columns to build users and time_tables. 
    Reads in both song_data and log_data and collects columns for songplays
    Result is written in parquet format and pushed to S3. 
    
    Parameters: 
    Spark session
    input_data = log_data where the songs sits on S3. 
    output_data = Path where the parquet files will be written to
    
    '''
    #setting filepath to log data file
    log_data = inputdata + 'log_data/*.json'

    #reading log data file
    df = spark.read.json(log_data)
    
    #filtering by actions for song plays
    actions_df = df.filer(df.page=='NextPage')\
                    .select('ts', 'userId','level','song'
                           'artist', 'sessionId', 'location', 'userAgent')

    #extracting columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 
                           'gender', 'level')\
                      .dropDuplicates()
    
    users_table.CreateorReplaceTempView('users')
    
    #writing users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    #creating timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    actions_df = actions_df.withColumn('timestamp', get_timestamp(actions_df.ts))
    
    #creating datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    actions_df = actions_df.withColumn('datetime', get_datetime(actions_df.ts))
    
    #extracting columns to create time table
    time_table = actions_df.select('datetime') \
                           .withColumn('start_time', actions_df.datetime) \
                           .withColumn('hour', hour('datetime')) \
                           .withColumn('day', dayofmonth('datetime')) \
                           .withColumn('week', weekofyear('datetime')) \
                           .withColumn('month', month('datetime')) \
                           .withColumn('year', year('datetime')) \
                           .withColumn('weekday', dayofweek('datetime')) \
                           .dropDuplicates()
    
    #writing time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                    .parquet(os.path.join(output_data,
                                          'time/time.parquet'), 'overwrite')
    
    #reading song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    #creating songs and logs datasets
    actions_df = actions_df.alias('log_df') 
    song_df = song_df.alias('song_df')
    
    joined_df = actions_df.join(song_df, col('log_df.artist') == col('song_df.artist_name'), 'inner')

    #extracting columns from joined song and log datasets to create songplays table 
    songplays_table = joined_df.select(
                        col('log_df.datetime').alias('start_time'), 
                        col('log_df.userId').alias('user_id'), 
                        col('log_df.level').alias('level'),
                        col('song_df.song_id').alias('song_id'), 
                        col('song_df.artist_id').alias('artist_id'), 
                        col('log_df.sessionId').alias('session_id'), 
                        col('log_df.location').alias('location'), 
                        col('log_df.userAgent').alias('user_agent'), 
                        year('log_df.datetime').alias('year'),
                        month('log_df.datetime').alias('month')) \
                        .withColumn('songplay_id', monotonically_increasing_id());
                        
    #writing songplays table to parquet files partitioned by year and month                   
    songplays_table.write.partitionBy('year','month')\
                    .parquet(os.path.join(output_data,'songplays/songplays.parquet'), 'overwrite')       


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
