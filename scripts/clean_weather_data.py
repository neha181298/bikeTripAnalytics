import os
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_weather_data(raw_data_dir: str, cleaned_data_dir: str) -> None:
    """
    Perform cleaning checks on the weather data (missing values, outliers, etc.)
    """
    raw_data_filepath = os.path.join(raw_data_dir, "weather_data.csv")
    df = pd.read_csv(raw_data_filepath, parse_dates=['date'])
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Check for missing values in critical columns
    required_columns = ['tavg', 'tmin', 'tmax', 'prcp', 'date']
    if df[required_columns].isnull().any().any():
        logger.warning("Missing values found in critical columns.")

    # Remove rows with missing date or coordinates
    df = df.dropna(subset=['date', 'tavg', 'prcp'])

    # Check for unrealistic values (e.g., negative temperature in summer months)
    df = df[df['tmax'] >= -100]  # Remove temperatures that are too low to be realistic

    # Remove duplicate rows based on city and date
    df = df.drop_duplicates(subset=['city', 'date'])

    # Generate the expected date range from the min to the max date in the data
    min_date = df['date'].min()
    max_date = df['date'].max()
    expected_dates = pd.date_range(start=min_date, end=max_date, freq='D').date

    # Find missing dates
    missing_dates = set(expected_dates) - set(df['date'])
    if missing_dates:
        logger.warning(f"Missing dates found: {sorted(missing_dates)}")

    # Save the cleaned DataFrame to a new CSV file
    cleaned_data_filepath = os.path.join(cleaned_data_dir, "weather_data.csv")
    df.to_csv(cleaned_data_filepath, index=False)
    logger.info(f"Cleaned weather data saved to {cleaned_data_filepath}")


if __name__ == "__main__":
    # Fetch and print the weather data for the specified cities
    # Get the directory of the script file
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # Define root_output_dir to be outside the script's directory
    raw_data_dir = os.path.join(script_dir, "..", "raw_data")
    cleaned_data_dir = os.path.join(script_dir, "..", "cleaned_data")

    # Normalize the path to resolve any '..' components
    raw_data_dir = os.path.abspath(raw_data_dir)
    cleaned_data_dir = os.path.abspath(cleaned_data_dir)

    weather_data = clean_weather_data(raw_data_dir, cleaned_data_dir)