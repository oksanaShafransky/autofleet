from fastapi import FastAPI
import pandas as pd
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from geo_utils import get_haversine_distance_spark
from read_and_transform_data import prepare_vehicle_data
app = FastAPI()
spark = SparkSession.builder.getOrCreate()

INPUT_PATH = abspath('..\input_data\vehicles_location_traces.csv')
OUTPUT_PATH = abspath('..\output_data')
VEHICLE_RIDES_PATH = f'{OUTPUT_PATH}\location_traces'
VEHICLE_LOCATION_PATH = f'{OUTPUT_PATH}\vehicle_rides'

#prepare all vehicle input_data on the output path
prepare_vehicle_data(spark, INPUT_PATH, OUTPUT_PATH)

@app.get("/events/velocity/average/{vehicleID}")
async def get_average_velocity_endpoint(vehicleID: str):
    """
    get_average_velocity_endpoint
    :param vehicleID:
    :return: Returns average velocity of a specified vehicle.
    """
    # read the Parquet input_data
    df = pd.read_parquet(VEHICLE_LOCATION_PATH)

    # filter the DataFrame to select only the rows where the vehicleID matches the specified vehicleID
    filtered_df = df[df['vehicle_id'] == vehicleID]

    # Calculate average velocity of the vehicleID
    avg_velocity = filtered_df['velocity'].mean()

    # return the filtered DataFrame as a JSON response
    return {"vehicle_id": vehicleID, "average_velocity": avg_velocity}

@app.get("/events/velocity/outliers/{vehicleID}")
async def get_velocity_outliers(vehicleID: str):
    """
    get_velocity_outliers
    :param vehicleID:
    :return: Returns a list of all the events of a vehicle where the velocity was below 5 KM/hour or above 150 KM/hour.
    """
    # read the Parquet input_data
    df = pd.read_parquet(VEHICLE_LOCATION_PATH)
    # filter the DataFrame to select only the rows where the vehicleID matches the specified vehicleID
    filtered_df = df[df["vehicle_id"] == vehicleID]
    # filter the DataFrame to select only the rows where velocity was below 5 KM/hour or above 150 KM/hour
    outliers_df = filtered_df[(filtered_df["velocity"] < 5) | (filtered_df["velocity"] > 150)]

    # return the filtered DataFrame as a JSON response
    return outliers_df.to_dict(orient="records")


@app.get("/rides/completed/{vehicleID}")
async def get_completed_rides(vehicleID: str):
    """
    get_completed_rides
    :param vehicleID:
    :return: Returns a list of all the completed rides of a specified vehicle.
    A ride is considered completed when the final location of a vehicle is within 15 meters from destination.
    """
    # read the Parquet input_data
    rides_df = spark.read.parquet(VEHICLE_RIDES_PATH)

    # calculate the distance between the final location of the vehicle and the destination for each ride
    # filter the DataFrame to select only the rows where the vehicleID matches the specified vehicleID
    # filter the DataFrame to select only the rows where the distance is less than or equal to 15 meters
    completed_rides_df = get_haversine_distance_spark(rides_df, 'dest_lat', 'dest_lng', 'finish_lat', 'finish_lng', 'distance_to_destination') \
        .filter(F.col('vehicle_id')==vehicleID)\
        .filter(F.col('distance_to_destination')<=15)
    completed_rides_df = completed_rides_df.toPandas()

    # return the filtered DataFrame as a JSON response
    return completed_rides_df.to_dict(orient='records')