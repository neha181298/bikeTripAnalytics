import logging
import os
import io
import zipfile
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Optional
from config import BIKESHARE_DATASOURCES

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_bikeshare_data(s3_bucket_url, filenames, raw_data_dir):
    """
    Ingests bikeshare data from various sources and saves them to CSV files.

    Parameters:
    -----------
    s3_bucket_url (str): The base URL of the S3 bucket where the data is stored.
    filenames (list): A list of filenames to download and process.
    output_dir (str): The directory where the processed CSV files will be saved.
    
    Returns:
    --------
    bool: Status indicating the success (True) or failure (False) of the process.
    """

    os.makedirs(raw_data_dir, exist_ok=True)

    for filename in filenames:
        file_url = s3_bucket_url + filename
        month = filename.split("-")[0]  # e.g., "202409"
        month_dir = os.path.join(raw_data_dir, month)
        os.makedirs(month_dir, exist_ok=True)

        logger.info(f"Downloading: {filename}")
        response = requests.get(file_url)
        response.raise_for_status()
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))

        # Get CSVs, ignoring system files
        csv_filenames = [f for f in zip_file.namelist()
                         if f.endswith(".csv") and "__MACOSX" not in f]

        logger.info(f"Found {len(csv_filenames)} CSV files for {month}: {csv_filenames}")
        monthly_dfs = []

        if csv_filenames:
            csv_name = csv_filenames[0]  # Only process the first CSV
            extracted_path = zip_file.extract(csv_name, path=month_dir)
            full_path = os.path.join(month_dir, os.path.basename(csv_name))

            # Move file to correct location if nested
            if extracted_path != full_path:
                os.rename(extracted_path, full_path)

            # Read and store the DataFrame
            df = pd.read_csv(full_path)
            monthly_dfs.append(df)

        # Combine and save
        if monthly_dfs:
            combined_df = pd.concat(monthly_dfs, ignore_index=True)
            combined_csv_path = os.path.join(month_dir, f"{month}-combined.csv")
            combined_df.to_csv(combined_csv_path, index=False)
            logger.info(f"Saved combined CSV for {month}: {combined_csv_path}")
        else:
            logger.info(f"No valid CSVs found for {month}.")


def ingest_city_station_data(
    endpoint: str,
    city: str,
    source_type: str,
    raw_data_dir: str,
    filename: Optional[str] = None,
    limit: int = 50000,
) -> str:
    """
    Fetches bikeshare station information data from a specified endpoint and saves it to a CSV file.

    Parameters:
    -----------
    endpoint (str): The URL endpoint to fetch the data from.
    city (str): The name of the city for which to fetch the data.
    source_type (str): The type of data source ('gbfs', 'soda', or 'excel').
    filename (str): The name of the output CSV file. If None, defaults to '{city}_stations.csv'.
    limit (int): The number of records to fetch per request (for 'soda' source).
    output_dir (str): The base directory where the CSV file will be saved.

    Returns:
    --------
    str: The path to the saved CSV file.

    """
    os.makedirs(raw_data_dir, exist_ok=True)
    if filename is None:
        filename = "stations.csv"
    output_path = os.path.join(raw_data_dir, filename)

    xlsx_path = None
    source = source_type.lower()

    if source == "soda":
        offset, records = 0, []
        while True:
            url = f"{endpoint}?$limit={limit}&$offset={offset}"
            resp = requests.get(url)
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            records.extend(batch)
            offset += limit
        df = pd.DataFrame(records)

    elif source == "gbfs":
        resp = requests.get(endpoint)
        resp.raise_for_status()
        js = resp.json()
        items = js.get("data", {}).get("stations", [])
        df = pd.DataFrame(items)

    elif source == "excel":
        resp = requests.get(endpoint)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        links = [
            a["href"]
            for a in soup.find_all("a", href=True)
            if a["href"].lower().endswith(".xlsx")
        ]
        if not links:
            raise Exception("No .xlsx links found on the page.")
        href = links[0]
        xlsx_url = (
            href
            if href.startswith(("http://", "https://"))
            else urljoin(endpoint, href)
        )
        xlsx_name = os.path.basename(xlsx_url)
        xlsx_path = os.path.join(raw_data_dir, xlsx_name)
        with requests.get(xlsx_url, stream=True) as r:
            r.raise_for_status()
            with open(xlsx_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        # skip metadata row and use second row as header
        df = pd.read_excel(xlsx_path, header=1)

    else:
        raise ValueError(
            f"Unsupported source_type: {source_type!r}. Use 'gbfs', 'soda', or 'excel'."
        )

    # ensure column names are strings
    df.columns = df.columns.map(str)
    # standardize lat/lng names
    rename_map = {}
    for col in df.columns:
        lc = col.lower()
        if lc in ("latitude", "lat"):
            rename_map[col] = "lat"
        elif lc in ("longitude", "lon", "long", "lng"):
            rename_map[col] = "lng"
    if rename_map:
        df = df.rename(columns=rename_map)

    df.to_csv(output_path, index=False)
    if xlsx_path and os.path.exists(xlsx_path):
        os.remove(xlsx_path)
    logger.info(f"Saved {len(df)} records to {output_path}")

    return


def ingest_all_bikeshare_data(raw_data_dir: str) -> None:
    """
    Ingests all bikeshare data for the specified cities and saves them to CSV files.    
    Parameters:
    -----------
    root_output_dir (str): The base directory where the CSV files will be saved.
    """
    # Ensure the raw output directory exists
    os.makedirs(raw_data_dir, exist_ok=True)  

    logger.info("Ingesting all bikeshare data...")

    # Ingest trip data and station data for each city
    for city, config in BIKESHARE_DATASOURCES.items():
        trip_output_dir = os.path.join(raw_data_dir, city, "trip_data")
        os.makedirs(trip_output_dir, exist_ok=True)  # Ensure the output directory exists

        logger.info(f"Ingesting trip data for {city}")
        ingest_bikeshare_data(
            s3_bucket_url=config["trip_data"]["base_url"],
            filenames=config["trip_data"]["filenames"],
            raw_data_dir=trip_output_dir
        )
        logger.info(f"Trip data for {city} ingested successfully.")

        station_output_dir = os.path.join(raw_data_dir, city, "station_data")
        os.makedirs(station_output_dir, exist_ok=True)  # Ensure the output directory exists
        logger.info(f"Ingesting station data for {city}")
        ingest_city_station_data(
            endpoint=config["station_data"]["endpoint"],
            city=city,
            source_type=config["station_data"]["source_type"],
            raw_data_dir=station_output_dir,
            filename=config.get("station_data", {}).get("filename", None)
        )
        
        logger.info(f"Station data for {city} ingested successfully.")
    
    logger.info("All bikeshare data ingested successfully.")


if __name__ == "__main__":
    # Get the directory of the script file
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # Define root_output_dir to be outside the script's directory
    raw_data_dir = os.path.join(script_dir, "..", "raw_data")

    # Normalize the path to resolve any '..' components
    raw_data_dir = os.path.abspath(raw_data_dir)

    ingest_all_bikeshare_data(raw_data_dir)
    
