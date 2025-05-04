import os
import logging
from config import BIKESHARE_DATASOURCES, WEATHER_CONFIG
from ingest_weather_data import ingest_weather_data
from ingest_bike_data import ingest_all_bikeshare_data
from clean_bike_data import process_all_cities_data
from clean_weather_data import clean_weather_data
from create_final_dataset import create_final_dataset

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def orchestrate_data_pipeline(raw_data_dir: str, cleaned_data_dir: str) -> None:
    """
    Orchestrates the data pipeline by calling the necessary functions in order.
    
    Parameters:
    -----------
    root_output_dir (str): Directory to save the output files.
    
    Returns:
    --------
    None
    """
    
    # Step 1: Ingest and clean weather data
    try:
        logger.info("Starting weather data ingestion...")
        ingest_weather_data(raw_data_dir)
        logger.info("Weather data ingestion completed successfully.")

        logger.info("Starting weather data cleaning...")
        clean_weather_data(raw_data_dir, cleaned_data_dir)
        logger.info("Weather data cleaning completed successfully.")

    except Exception as e:
        logger.error(f"Error during weather data processing: {e}")
        return  # Stop further execution if any weather step fails

    # Step 2: Ingest and clean bike data
    try:
        logger.info("Starting bike share data ingestion...")
        ingest_all_bikeshare_data(raw_data_dir)
        logger.info("Bike share data ingestion completed successfully.")

        logger.info("Starting bike data cleaning...")
        process_all_cities_data(raw_data_dir, cleaned_data_dir)
        logger.info("Bike data cleaning completed successfully.")

    except Exception as e:
        logger.error(f"Error during bike data processing: {e}")
        return  # Stop further execution if any bike step fails

    # Step 3: Create final dataset
    try:
        logger.info("Starting final dataset creation...")
        create_final_dataset(cleaned_data_dir)
        logger.info("Final dataset creation completed successfully.")

    except Exception as e:
        logger.error(f"Error during final dataset creation: {e}")
        return  # Stop further execution if final dataset creation fails



if __name__ == "__main__":  
    # Get the directory of the script file
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # Define root_output_dir to be outside the script's directory
    root_output_dir = os.path.join(script_dir, "..")

    raw_data_dir = os.path.join(root_output_dir, "raw_data")
    cleaned_data_dir = os.path.join(root_output_dir, "cleaned_data")

    # Normalize the path to resolve any '..' components
    raw_data_dir = os.path.abspath(raw_data_dir)
    cleaned_data_dir = os.path.abspath(cleaned_data_dir)

    # Ensure the raw and cleaned data directories exist
    os.makedirs(os.path.join(root_output_dir, "raw_data"), exist_ok=True)
    os.makedirs(os.path.join(root_output_dir, "cleaned_data"), exist_ok=True)

    # Run the data pipeline
    orchestrate_data_pipeline(raw_data_dir, cleaned_data_dir)
    