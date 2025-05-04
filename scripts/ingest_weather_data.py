import warnings
import logging
import os
import pandas as pd
from meteostat import Daily, Point
from datetime import datetime
from config import WEATHER_CONFIG

warnings.simplefilter(action='ignore', category=FutureWarning)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_weather_data(raw_data_dir, city_config: dict = WEATHER_CONFIG) -> pd.DataFrame:
    """
    Fetches weather data for multiple cities and returns a consolidated DataFrame.
    Includes error handling for network, data, and parsing issues.
    """
    output_path = os.path.join(raw_data_dir,"weather_data.csv")
    # 1. Initialize a list to store all DataFrames
    all_dfs = []

    # 2. Fetch, tag, and collect each city’s DataFrame using the config
    for city, config in city_config.items():
        try:
            # Extract the city's details from the config
            city_label = config["label"]
            coordinates = config["coordinates"]
            start_date = config["start_date"]
            end_date = config["end_date"]

            # Fetch the weather data for the city
            df = Daily(coordinates, start_date, end_date).fetch()

            # Check if data is empty
            if df.empty:
                logger.warning(f"No data returned for {city} between {start_date} and {end_date}.")
                continue  # Skip this city if no data is returned

            # Prepare the DataFrame: reset index, rename date column, and add city label
            df = df.reset_index().rename(columns={"time": "date"})  # bring the date index into a column
            df["city"] = city_label

            # Ensure the 'date' column is datetime type
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

            # Add the DataFrame to the list of all DataFrames
            all_dfs.append(df)

        except Exception as e:
            logger.error(f"Error fetching data for {city}: {e}")
            continue  # Skip this city and continue with the next one

    if not all_dfs:
        logger.error("No valid data fetched for any city.")
        return pd.DataFrame()  # Return an empty DataFrame if no valid data was fetched

    # 3. Concatenate all DataFrames into one
    try:
        weather_df = pd.concat(all_dfs, ignore_index=True)
    except Exception as e:
        logger.error(f"Error concatenating data frames: {e}")
        return pd.DataFrame()

    # 4. (Optional) Reorder columns to place 'city' and 'date' first
    cols = ["city", "date"] + [c for c in weather_df.columns if c not in ("city", "date")]
    weather_df = weather_df[cols]

    # 5. Strip off the time component from the 'date' column (optional)
    weather_df['date'] = weather_df['date'].dt.date

    # 6. Save the DataFrame to a CSV file
    weather_df.to_csv(output_path, index=False)
    logger.info(f"Weather data saved to {output_path}")


if __name__ == "__main__":
    # Fetch and print the weather data for the specified cities
    # Get the directory of the script file
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # Define root_output_dir to be outside the script's directory
    raw_data_dir = os.path.join(script_dir, "..", "raw_data")

    # Normalize the path to resolve any '..' components
    raw_data_dir = os.path.abspath(raw_data_dir)

    ingest_weather_data(raw_data_dir)
