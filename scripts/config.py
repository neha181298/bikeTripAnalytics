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

