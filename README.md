# Data Engineer Take-home Assignment  
A take-home assignment for DE profiles.

## Introduction

The goal of this take-home assignment is for us to get a better understanding of 
your technical skill set and how you approach and solve a task.

## Instructions summary

In this assignment, you'll be working with a dataset that contains GPS signals of vehicles over a period of time and the rides' destinations.
The goal is to extract some insights from this dataset. 
To this end, we'd like you implement an ETL job in Python (using PySpark/Pandas or any framework of your choice) to process the data 
and implement a simple API to showcase your insights.  

The job should do the following:
1. Read the input dataset into a PySpark/Pandas DataFrame. See [Input data](#input-data) section for more details.
2. Transform the data set according to the instructions provided in the section [Transform data](#transform-data).
3. Write the results to file. See [Output data](#output-data) section for more details. 
4. Implement an API that consumes the result. See [Consume data](#consume-data) section for more details.

## Input data

Description of columns in the file:
- vehicle_id - ID of a vehicle
- event_timestamp - trace sampling timestamp
- lat - current latitude
- lng - current longitude
- dest_lat - the latitude of the destination
- dest_lng - the longitude of the destination


## Transform data
Your task is to transform the input data into 2 tables, each described below.

#### **Table 1: Exploring vehicles' location traces throughout time**

We'd like to calculate the distance and the average velocity of a vehicle between two consecutive coordinates.
Use [Haversine formula](https://en.wikipedia.org/wiki/Haversine_formula) to calculate the distance between two coordinates, 
relevant functions provided in `geo_utils.py` (Pyspark and Pandas), but feel free to implement this calculation however you'd like.

#### **Table 2: Exploring vehicles' rides**
We'd also like a summary for separate vehicle rides (according to final destinations). 
For each vehicle destination, a ride starts with the first event and ends with the last one.
It doesn't mean that the ride was completed or arrived to its final destination.

The final dataset will contain the following columns:
- vehicle_id
- dest_lat
- dest_lng
- start_time
- source_lat
- source_lng
- finish_time
- finish_lat
- finish_lng
- total_distance (calculate from source to destination)
- ride_distance (calculate from source to finish)
- ride_duration


## Output data

Once you're done with the transformation step, the next step is to write the data to a file. 


#### Bonus: 
Choose optimized format to store your data.


## Consume data
Now we'd like to consume your results through API. The API should be built using a web framework of your choice, such as FastAPI or Flask.

#### The API should have the following endpoints:

- `GET /events/velocity/average/{vehicleID}`: Returns average velocity of a specified vehicle.
- `GET /events/velocity/outliers/{vehicleID}`: Returns a list of all the events of a vehicle where the velocity was below 5 KM/hour or above 150 KM/hour.
- `GET /rides/completed/{vehicleID}`: Returns a list of all the completed rides of a specified vehicle. A ride is considered completed when the final location of a vehicle is within 15 meters from destination.


## Additional Considerations

- Provide clear and concise documentation for the data pipeline and the API (including instructions for running them locally).
- Use version control to manage the codebase and collaborate with team members.
- Think about how would you automate this pipeline.


## Contact
If you have any questions, don't hesitate to reach out to us :)  
Michal - michalbarer@autofleet.io
