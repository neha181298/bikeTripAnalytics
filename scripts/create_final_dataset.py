import pandas as pd
import os
import logging
from datetime import datetime
from config import BIKESHARE_DATASOURCES

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def aggregate_bike_trip_data(cleaned_data_dir: str) -> pd.DataFrame:
    """
    Aggregates bike trip data by city and date.
    
    Parameters:
    -----------
    df_trips (pd.DataFrame): DataFrame containing bike trip data.
    
    Returns:
    --------
    pd.DataFrame: Aggregated DataFrame with total trips per city and date.
    """
    cities = BIKESHARE_DATASOURCES.keys()  # Get the list of cities from the config

    all_dfs = []
    # Iterate through the list of cities
    for city in cities:
        # Create a directory for each city
        city_file_path = os.path.join(cleaned_data_dir, city, f"{city}_cleaned_trips.csv")
        if not os.path.exists(city_file_path):
            logger.warning(f"File not found: {city_file_path}. Please check the file path.")
            continue
        # Read the cleaned trip data for the city
        df_city = pd.read_csv(city_file_path, parse_dates=['started_at', 'ended_at'])  

        # Extract the date from the start time
        df_city['date'] = df_city['started_at'].dt.date   
        df_city['city'] = city  # Add a new column for the city

        df_city_aggregated_data = df_city.groupby(['city', 'date']).agg(
            ride_count         = ('ride_id',                'count'),
            member_count       = ('member_casual', lambda x: (x == 'member').sum()),
            casual_count       = ('member_casual', lambda x: (x == 'casual').sum()),
        ).reset_index()

        all_dfs.append(df_city_aggregated_data)

    # Concatenate all DataFrames into one
    df_trips = pd.concat(all_dfs, ignore_index=True)      
    
    # Save the aggregated DataFrame to a CSV file
    output_path = os.path.join(cleaned_data_dir, "aggregated_bike_data.csv")
    df_trips.to_csv(output_path, index=False)
    logger.info(f"Aggregated bike trip data saved to {output_path}")
        

def merge_bike_and_weather_data(cleaned_data_dir: str):
    """
    Merges bike trip data with weather data.
    
    Parameters:
    -----------
    df_bike (pd.DataFrame): DataFrame containing bike trip data.
    df_weather (pd.DataFrame): DataFrame containing weather data.
    
    Returns:
    --------
    pd.DataFrame: Merged DataFrame with bike trip and weather data.
    """
    df_bike = pd.read_csv(os.path.join(cleaned_data_dir, "aggregated_bike_data.csv"), parse_dates=['date'])
    df_weather = pd.read_csv(os.path.join(cleaned_data_dir, "weather_data.csv"), parse_dates=['date'])

    # Merge the two DataFrames on city and date
    merged_data = pd.merge(df_bike, df_weather, on=['city', 'date'], how='left')
    
    # Save the merged DataFrame to a CSV file
    output_path = os.path.join(cleaned_data_dir, "merged_bike_weather_data.csv")
    merged_data.to_csv(output_path, index=False)
    logger.info(f"Merged bike and weather data saved to {output_path}")


def create_final_dataset(cleaned_data_dir: str):
    """
    Creates the final dataset by aggregating bike trip data and merging it with weather data.
    
    Parameters:
    -----------
    cleaned_data_dir (str): Directory containing cleaned data files.
    
    Returns:
    --------
    pd.DataFrame: Final dataset with bike trip and weather data.
    """
    # Aggregate bike trip data
    aggregate_bike_trip_data(cleaned_data_dir)

    # Merge bike trip data with weather data
    merge_bike_and_weather_data(cleaned_data_dir)


if __name__ == "__main__":  
    # Get the directory of the script file
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # Define root_output_dir to be outside the script's directory
    cleaned_data_dir = os.path.join(script_dir, "..", "cleaned_data")

    # Normalize the path to resolve any '..' components
    cleaned_data_dir = os.path.abspath(cleaned_data_dir)

    create_final_dataset(cleaned_data_dir)
