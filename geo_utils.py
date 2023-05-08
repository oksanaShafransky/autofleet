from pyspark.sql import functions as F
import numpy as np

from sklearn.metrics.pairwise import haversine_distances

UNITS_DICT = {
    'KM': 1000,
    'M': 1
}


def get_haversine_distance_spark(df, src_lat_col: str, src_lng_col: str,
                                 dest_lat_col: str, dest_lng_col: str, dest_col_name: str, unit='KM'):
    """
    Calculates haversine distance between source and destination columns and places the result in a new column.

    :param df: Pyspark dataframe
    :param src_lat_col: Source latitude column
    :param src_lng_col: Source longitude column
    :param dest_lat_col: Destination latitude column
    :param dest_lng_col: Destination longitude column
    :param dest_col_name: Column name that holds the calculation
    :param unit: Calculation unit, defaults to Kilometers
    :return: Dataframe with calculated distance column
    """
    origin_lat_rad_col = F.radians(src_lat_col)
    origin_lng_rad_col = F.radians(src_lng_col)
    dest_lat_rad_col = F.radians(dest_lat_col)
    dest_lng_rad_col = F.radians(dest_lng_col)

    diff_lat = origin_lat_rad_col - dest_lat_rad_col
    diff_lng = origin_lng_rad_col - dest_lng_rad_col
    radius = 6371000

    area = F.sin(diff_lat / 2) ** 2 + F.cos(origin_lat_rad_col) * F.cos(dest_lat_rad_col) * F.sin(diff_lng / 2) ** 2
    central_angle = 2 * F.asin(F.sqrt(area))
    distance = (central_angle * radius) / UNITS_DICT[unit]

    calc_distance = F.when(F.col(src_lat_col).isNotNull() & F.col(src_lng_col).isNotNull() &
                           F.col(dest_lat_col).isNotNull() & F.col(dest_lng_col).isNotNull(),
                           distance).otherwise(None)
    df = df.withColumn(dest_col_name, calc_distance)

    return df


def get_haversine_distance_pandas(df, src_lat_col: str, src_lng_col: str, dest_lat_col: str, dest_lng_col: str,
                                  dest_col_name: str, unit='KM', batch_size=1000):
    """
    Calculates haversine distance between source and destination columns and places the result in a new column.

    :param df: Pandas dataframe
    :param src_lat_col: Source latitude column
    :param src_lng_col: Source longitude column
    :param dest_lat_col: Destination latitude column
    :param dest_lng_col: Destination longitude column
    :param dest_col_name: Column name that holds the calculation
    :param unit: Calculation unit, defaults to Kilometers
    :param batch_size: Calculation batch size
    :return: Dataframe with calculated distance column
    """

    def matrix_haversine_distance(sources, destinations, unit):
        x = np.radians(sources)
        y = np.radians(destinations)
        result = haversine_distances(x, y)
        return result * 6371000 / UNITS_DICT[unit]

    distances = []
    for i in range(0, len(df), batch_size):
        d = matrix_haversine_distance(
            df.iloc[i: i + batch_size][[src_lat_col, src_lng_col]],
            df.iloc[i: i + batch_size][[dest_lat_col, dest_lng_col]],
            unit
        ).diagonal()
        distances.extend(d)
    df[dest_col_name] = distances
    return df
