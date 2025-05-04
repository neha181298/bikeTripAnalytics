# Bike Trip Analytics

## Overview

This project analyzes bike trip data, combining it with weather information to identify patterns and insights. The workflow involves ingesting data from various sources, cleaning and transforming it, and then merging it into a final dataset for analysis.

## Repository Structure

*   `raw_data/`: Stores the raw data ingested from various sources.
*   `cleaned_data/`: Stores the cleaned and transformed data.
*   `scripts/`: Contains the Python scripts for data ingestion, cleaning, transformation, and orchestration.
*   `config.py`: Contains configuration settings for data sources, API keys, and file paths.
*   `requirements.txt`: Lists the Python packages required to run the project.

## Prerequisites

*   **Python 3.9+**

## Setup and Installation

1.  **Clone the repository:**

    ```bash
    git clone <your_repository_url>
    cd Bike Trip Analytics
    ```

2.  **Install dependencies:**

    *   **Using pip:**

        ```bash
        pip install -r requirements.txt
        ```

## Data Ingestion

The project ingests both bike trip data and weather data.

### Bike Trip Data

*   The `ingest_bike_data.py` script downloads bike trip data from various sources (GBFS, SODA, Excel files).  The data sources and file names are defined in the `BIKESHARE_DATASOURCES` dictionary in `config.py`.
*   To ingest bike trip data, run the following command:

    ```bash
    python scripts/ingest_bike_data.py
    ```

### Weather Data

*   The `ingest_weather_data.py` script retrieves weather data from an API or a local file, as configured in the `WEATHER_CONFIG` dictionary in `config.py`.  It uses the `requests` and `pandas` libraries to fetch and parse the data.  The `meteostat` library is also used, suggesting integration with the Meteostat API or data.
*   To ingest weather data, run the following command:

    ```bash
    python scripts/ingest_weather_data.py
    ```

## Data Cleaning and Transformation

The project includes scripts for cleaning and transforming both bike trip data and weather data.

### Bike Trip Data Cleaning

*   The `clean_bike_data.py` script cleans and transforms the bike trip data.  This might involve removing duplicates, handling missing values, standardizing column names, and converting data types.
*   To clean the bike trip data, run the following command:

    ```bash
    python scripts/clean_bike_data.py
    ```

### Weather Data Cleaning

*   The `clean_weather_data.py` script cleans and transforms the weather data.  This might involve handling missing values, converting units, and extracting relevant features.
*   To clean the weather data, run the following command:

    ```bash
    python scripts/clean_weather_data.py
    ```

## Data Merging and Final Dataset Creation

*   The `create_final_dataset.py` script merges the cleaned bike trip data and weather data into a final dataset.  This script likely performs a join operation based on date, time, and location.
*   To create the final dataset, run the following command:

    ```bash
    python scripts/create_final_dataset.py
    ```

## Workflow Orchestration

The `workflow_orchestrator.py` script orchestrates the entire data pipeline, calling the data ingestion, cleaning, and merging scripts in the correct order.  It also includes error handling to ensure that the pipeline runs smoothly.

*   To run the entire workflow, execute:

    ```bash
    python scripts/workflow_orchestrator.py
    ```

## Output

The raw data is stored in the `raw_data/` directory, the cleaned data is stored in the `cleaned_data/` directory, and the final dataset is stored in a location determined by the `create_final_dataset.py` script (likely also within the project directory).

## Error Handling

The `workflow_orchestrator.py` script includes basic error handling. If any step in the pipeline fails, the script will log an error message and stop further execution. More sophisticated error handling (e.g., sending email notifications, retrying failed tasks) can be added as needed.

## Contributing

Contributions to this project are welcome! Please submit a pull request with your changes.

