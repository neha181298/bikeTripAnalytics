import os
import logging
from typing import List, Dict
import pandas as pd
import numpy as np
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from pandera.errors import SchemaError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def filter_duplicates(city, df_trips: pd.DataFrame) -> pd.DataFrame:
    """
    Removes duplicate entries based on the `ride_id` column.
    
    Parameters:
    -----------
    df (pd.DataFrame): The DataFrame containing the trip data.
    
    Returns:
    --------
    pd.DataFrame: DataFrame with duplicates removed based on `ride_id`.
    """
    df_cleaned = df_trips.drop_duplicates(subset=["ride_id"])
    logger.info(f"{city}: Removed {len(df_trips) - len(df_cleaned)} duplicate entries based on ride_id.")
    return df_cleaned


def filter_missing_values(city, df_trips: pd.DataFrame) -> pd.DataFrame:
    """
    Handles missing values in critical columns.
    
    Parameters:
    -----------
    df (pd.DataFrame): The DataFrame containing the trip data.
    
    Returns:
    --------
    pd.DataFrame: DataFrame with missing values handled.
    """
    df_cleaned = df_trips.dropna(subset=["ride_id", "started_at", "ended_at", "start_lat", "start_lng", "end_lat", "end_lng"])
    logger.info(f"{city}: Removed {len(df_trips) - len(df_cleaned)} rows with missing critical columns.")
    return df_cleaned


def filter_trips_by_geolocation_bounds(
    city: str,
    df_trips: pd.DataFrame,
    df_stations: str,
) -> pd.DataFrame:
    """
    Filters trips based on geolocation bounds defined by the station data.
    Parameters:
    ----------
    city (str): The name of the city.
    df_trips (pd.DataFrame): DataFrame containing trip data.
    df_stations (pd.DataFrame): DataFrame containing station data.
    Returns:
    -------
    pd.DataFrame: DataFrame with trips filtered by geolocation bounds.
    """
    # Compute station-area bounds
    lat_min = df_stations["lat"].min()
    lat_max = df_stations["lat"].max()
    lng_min = df_stations["lng"].min()
    lng_max = df_stations["lng"].max()

    # Filter trips to only those within bounds
    mask = (
        df_trips["end_lat"].between(lat_min, lat_max) &
        df_trips["end_lng"].between(lng_min, lng_max)
    )
    total    = len(df_trips)
    kept     = mask.sum()
    outliers = total - kept
    
    # Report
    logger.info(f"{city}: {total} total trips, {outliers} out of bound trips removed, {kept} remain")
    
    # Return cleaned DataFrame
    return df_trips.loc[mask].reset_index(drop=True)


def filter_trips_by_duration(
    city: str,
    df_trips: pd.DataFrame,
    start_col: str = "started_at",
    end_col: str = "ended_at",
    min_minutes: float = 0,
    max_minutes: float = 24 * 60
) -> pd.DataFrame:
    """
    Filters trips based on their duration.
    Parameters:
    ----------
    city (str): The name of the city.
    df_trips (pd.DataFrame): DataFrame containing trip data.
    start_col (str): Column name for trip start time.
    end_col (str): Column name for trip end time.
    min_minutes (float): Minimum trip duration in minutes. Default is 0 minutes.
    max_minutes (float): Maximum trip duration in minutes. Default is 24 hours.
    Returns:
    -------
    pd.DataFrame: DataFrame with trips filtered by duration.    
    """
    df_trips[start_col] = pd.to_datetime(df_trips[start_col])
    df_trips[end_col] = pd.to_datetime(df_trips[end_col])
    # compute duration
    df_trips["trip_duration_minutes"] = (
        (df_trips[end_col] - df_trips[start_col])
        .dt.total_seconds() / 60
    )

    # build mask of valid durations
    mask = df_trips["trip_duration_minutes"].between(min_minutes, max_minutes)

    removed = len(df_trips) - mask.sum()
    logger.info(f"{city}: Removed {removed} trips outside duration [{min_minutes}, {max_minutes}] minutes")

    # Drop the trip_duration_minutes column as it's not needed anymore
    df_trips.drop(columns=["trip_duration_minutes"], inplace=True)

    # return only the valid rows
    return df_trips.loc[mask].reset_index(drop=True)


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points on the Earth
    specified in decimal degrees using the Haversine formula.

    Parameters:
    ----------
    lat1, lon1 : float
        Latitude and longitude of the first point.
    lat2, lon2 : float
        Latitude and longitude of the second point.

    Returns:
    -------
    float
        Distance between the two points in kilometers.  
    """
    R = 6371  # Earth radius in kilometers

    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)

    a = np.sin(delta_phi / 2.0) ** 2 + \
        np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2.0) ** 2

    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    return R * c  # returns distance in kilometers


def filter_trips_by_distance(
    city: str,
    df_trips: pd.DataFrame,
    start_lat: str = "start_lat",
    start_lng: str = "start_lng",
    end_lat:   str = "end_lat",
    end_lng:   str = "end_lng",
) -> pd.DataFrame:
    """
    Filters trips based on distance calculated using the Haversine formula.
    Parameters:
    ----------
    city (str): The name of the city.
    df_trips (pd.DataFrame): DataFrame containing trip data.
    start_lat (str): Column name for starting latitude.
    start_lng (str): Column name for starting longitude.
    end_lat (str): Column name for ending latitude.
    end_lng (str): Column name for ending longitude.
    Returns:
    -------
    pd.DataFrame: DataFrame with trips filtered by distance.
    """
    # Compute distance (km)
    df_trips["trip_distance_km"] = haversine_distance(
        df_trips[start_lat].values,
        df_trips[start_lng].values,
        df_trips[end_lat].values,
        df_trips[end_lng].values
    )

    # Zero and negative distance mask
    mask = df_trips["trip_distance_km"] > 0

    removed = len(df_trips) - mask.sum()
    logger.info(f"{city}: Removed {removed} trips with zero or negative distance")

    # Drop the trip_distance_km column as it's not needed anymore
    df_trips.drop(columns=["trip_distance_km"], inplace=True)

    # return only the valid rows
    return df_trips.loc[mask].reset_index(drop=True)


def process_and_clean_city_data(
    city: str,
    trip_data_path: str,
    station_data_path: str,
    cleaned_data_dir: str
):
    """
    Process and clean bike trip data for a specific city.
    Parameters:
    ----------
    city (str): The name of the city.
    trip_data_path (str): Path to the trip data CSV file.
    station_data_path (str): Path to the station data CSV file.
    cleaned_data_dir (str): Directory to save cleaned data. 
    """
    # Load trip and station data for the city
    logger.info(f"Cleaning data for {city}...")

    df_trips = pd.read_csv(trip_data_path)
    df_stations = pd.read_csv(station_data_path)

    # Replace 0.0 with NaN in coordinate columns, 0.0 represent missing coordinates
    coord_cols = ["start_lat", "start_lng", "end_lat", "end_lng"]
    df_trips[coord_cols] = df_trips[coord_cols].replace(0.0, np.nan)

    # Now assign correct data types after cleaning
    logger.info(f"Assigning correct data types for {city}")
    df_trips["started_at"] = pd.to_datetime(df_trips["started_at"], errors='coerce')
    df_trips["ended_at"] = pd.to_datetime(df_trips["ended_at"], errors='coerce')

    # Convert coordinate columns to float using pd.to_numeric for proper error handling
    df_trips["start_lat"] = pd.to_numeric(df_trips["start_lat"], errors='coerce')
    df_trips["start_lng"] = pd.to_numeric(df_trips["start_lng"], errors='coerce')
    df_trips["end_lat"] = pd.to_numeric(df_trips["end_lat"], errors='coerce')
    df_trips["end_lng"] = pd.to_numeric(df_trips["end_lng"], errors='coerce')

    df_trips["ride_id"] = df_trips["ride_id"].astype(str)
    df_trips["start_station_id"] = df_trips["start_station_id"].astype(str)
    df_trips["end_station_id"] = df_trips["end_station_id"].astype(str)
    df_trips["rideable_type"] = df_trips["rideable_type"].astype(str)
    df_trips["member_casual"] = df_trips["member_casual"].astype(str)

    # Apply the filter functions
    logger.info(f"Filtering trips by duplicates for {city}")
    df_trips = filter_duplicates(city, df_trips)

    logger.info(f"Filtering trips by missing values for {city}")
    df_trips = filter_missing_values(city, df_trips)

    logger.info(f"Filtering trips by geolocation bounds for {city}")
    df_trips = filter_trips_by_geolocation_bounds(city, df_trips, df_stations)

    logger.info(f"Filtering trips by duration for {city}")
    df_trips = filter_trips_by_duration(city, df_trips)

    logger.info(f"Filtering trips by distance for {city}")
    df_trips = filter_trips_by_distance(city, df_trips)

    cleaned_schema = DataFrameSchema({
        "ride_id":            Column(str, unique=True, nullable=False),
        "rideable_type":      Column(str,             nullable=False),
        "started_at":         Column(pa.DateTime,     nullable=False),
        "ended_at":           Column(pa.DateTime,     nullable=False),
        "start_station_name": Column(str,             nullable=True),
        "start_station_id":   Column(str,             nullable=True),
        "end_station_name":   Column(str,             nullable=True),
        "end_station_id":     Column(str,             nullable=True),
        "start_lat":          Column(float,           nullable=False),
        "end_lat":            Column(float,           nullable=False),
        "start_lng":          Column(float,           nullable=False),
        "end_lng":            Column(float,           nullable=False),
        "member_casual":      Column(str, Check.isin(["member", "casual"]), nullable=False),
    })

    # --- 4. Run validation ---
    try:
        validated = cleaned_schema.validate(df_trips)
        logger.info(f"Cleaned data validated successfully for {city}.")
    except pa.errors.SchemaErrors as e:
        logger.debug(f"Data validation failed for {city}. Issues: {e.failure_cases}")

    # Store the cleaned data
    cleaned_city_dir = os.path.join(cleaned_data_dir, city)
    os.makedirs(cleaned_city_dir, exist_ok=True)

    cleaned_file_path = os.path.join(cleaned_city_dir, f"{city}_cleaned_trips.csv")
    df_trips.to_csv(cleaned_file_path, index=False)

    logger.info(f"Cleaned data for {city} saved to {cleaned_file_path}")


def process_all_cities_data(raw_data_dir: str, cleaned_data_dir: str):
    """
    Process and clean bike trip data for all cities.
    Parameters:
    ----------
    raw_data_dir (str): Directory containing raw data.
    cleaned_data_dir (str): Directory to save cleaned data.
    """
    os.makedirs(cleaned_data_dir, exist_ok=True)

    # Define the list of cities and their data paths
    cities = ["NYC", "Chicago", "Boston", "Capital"]
    month = "202409"

    # Loop through each city and process its data
    for city in cities:
        trip_data_path = os.path.join(raw_data_dir, city, "trip_data", month, f"{month}-combined.csv")
        station_data_path = os.path.join(raw_data_dir, city, "station_data", f"stations.csv")
        
        process_and_clean_city_data(city, trip_data_path, station_data_path, cleaned_data_dir)


if __name__ == "__main__":
    # Define root directories
    script_dir = os.path.dirname(os.path.realpath(__file__))
    raw_data_dir = os.path.join(script_dir, "..", "raw_data")
    cleaned_data_dir = os.path.join(script_dir, "..", "cleaned_data")
    process_all_cities_data(raw_data_dir, cleaned_data_dir)