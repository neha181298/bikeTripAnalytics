from meteostat import Point
from datetime import datetime

BIKESHARE_DATASOURCES = {
    "NYC": {
        "trip_data": {
            "base_url": "https://s3.amazonaws.com/tripdata/",
            "filenames": ["202409-citibike-tripdata.zip"],
        },
        "station_data": {
            "endpoint": "https://gbfs.citibikenyc.com/gbfs/en/station_information.json",
            "source_type": "gbfs",
        }
    },
    "Chicago": {
        "trip_data": {
            "base_url": "https://divvy-tripdata.s3.amazonaws.com/",
            "filenames": ["202409-divvy-tripdata.zip"],
        },
        "station_data": {
            "endpoint": "https://data.cityofchicago.org/resource/bbyy-e7gq.json",
            "source_type": "soda",
        }
    },
    "Boston": {
        "trip_data": {
            "base_url": "https://s3.amazonaws.com/hubway-data/",
            "filenames": ["202409-bluebikes-tripdata.zip"],
        },
        "station_data": {
            "endpoint": "https://bluebikes.com/system-data",        
            "source_type": "excel",
        }
    },
    "Capital": {
        "trip_data": {
            "base_url": "https://s3.amazonaws.com/capitalbikeshare-data/",
            "filenames": ["202409-capitalbikeshare-tripdata.zip"],
        },
        "station_data": {
            "endpoint": "https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json",
            "source_type": "gbfs",
        }
    }
}

# City-specific configuration (can be extended later)
WEATHER_CONFIG = {
    "NYC": {
        "label": "NYC",
        "coordinates": Point(40.7128, -74.0060),
        "start_date": datetime(2024, 8, 31),
        "end_date": datetime(2024, 9, 30),
    },
    "Chicago": {
        "label": "Chicago",
        "coordinates": Point(41.8781, -87.6298),
        "start_date": datetime(2024, 8, 31),
        "end_date": datetime(2024, 9, 30),
    },
    "Capital": {
        "label": "Capital",
        "coordinates": Point(38.9072, -77.0369),
        "start_date": datetime(2024, 8, 31),
        "end_date": datetime(2024, 9, 30),
    },
    "Boston": {
        "label": "Boston",
        "coordinates": Point(42.3601, -71.0589),
        "start_date": datetime(2024, 8, 31),
        "end_date": datetime(2024, 9, 30),
    }
}