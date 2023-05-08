from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession
from geo_utils import get_haversine_distance_spark
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window
from pyspark.sql import functions as F

#read input csv input_data into pyspark DataFrame
def read_input_data(spark: SparkSession, input_path: str) -> None:
    return spark.read.csv(input_path, header=True, inferSchema=True)


#Exploring vehicles' location traces throughout time.
#Calculate the distance and the average velocity of a vehicle between two consecutive coordinates.
def get_vehicle_location_traces(df:DataFrame) -> DataFrame:
    vehicle_with_distance_df = get_haversine_distance_spark(df, 'lat', 'lng', 'destination_lat', 'destination_lng', 'distance')
    vehicle_traces_df = vehicle_with_distance_df.withColumn('prev_timestamp', lag(col('event_timestamp')).over(Window.partitionBy('vehicle_id').orderBy('event_timestamp'))) \
        .withColumn('time_diff', F.coalesce(col('event_timestamp') - col('prev_timestamp'), F.lit(0)))\
        .withColumn('avg_velocity', F.coalesce(col('distance') / col('time_diff'), F.lit(0)))
    return vehicle_traces_df


#Exploring vehicles' rides
#Summary for separate vehicle rides (according to final destinations).
#For each vehicle destination, a ride starts with the first event and ends with the last one.
#It doesn't mean that the ride was completed or arrived to its final destination.
def get_vehicle_rides(df: DataFrame) -> DataFrame:
    df = df.groupBy('vehicle_id', 'destination_lat', 'destination_lng') \
        .agg(F.to_timestamp(F.first('event_timestamp')).alias('start'), \
             F.to_timestamp(F.last('event_timestamp')).alias('finish'), \
             F.first('lat').alias('source_lat'), \
             F.first('lng').alias('source_lng'), \
             F.last('destination_lat').alias('finish_lat'), \
             F.last('destination_lng').alias('finish_lng'), \
             ) \
        .withColumn('start_time', F.to_timestamp(F.col('start'))) \
        .withColumn('finish_time', F.to_timestamp(F.col('finish'))) \
        .withColumn('ride_duration', (F.col('finish_time').cast("long")-F.col('start_time').cast("long")))
    df = get_haversine_distance_spark(df, 'source_lat','source_lng','finish_lat','finish_lng', 'ride_distance')
    df = get_haversine_distance_spark(df, 'source_lat','source_lng','destination_lat','destination_lng', 'total_distance') \
        .withColumnRenamed('destination_lat','dest_lat') \
        .withColumnRenamed('destination_lng','dest_lng')

    return df.select('vehicle_id', 'dest_lat', 'dest_lng', 'source_lat', 'source_lng', \
                     'start_time', 'finish_time', 'finish_lat', 'finish_lng', \
                     'total_distance', 'ride_distance', 'ride_duration')


#store the input_data to parquet files
def write_data(df: DataFrame, output_path: str) -> None:
    df.write.parquet(output_path)


#To automate this pipeline, I would use Airflow, that will run this methods periodically
#and will prepare the updated data partitioned by date.
#The airflow will run 2 DAGs per each functionality (locations and rides).
def prepare_vehicle_data(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    This method preprocess all input input_data and transforms it to parquet format.
    Parquet is a columnar storage format that provides high compression ratios, efficient encoding schemes, and supports nested input_data structures.
    Parquet is performance efficient format to use with spark.
    :param spark:
    :param input_path:
    :param output_path:
    :return:
    """
    input_df = read_input_data(spark, input_path)
    location_df = get_vehicle_location_traces(input_df)
    write_data(location_df, f'{output_path}\location_traces')
    rides_df = get_vehicle_rides(input_df)
    write_data(rides_df, f'{output_path}\vehicle_rides')