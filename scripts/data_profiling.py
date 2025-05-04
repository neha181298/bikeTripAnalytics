import os
import logging
import pandas as pd
from ydata_profiling import ProfileReport  # formerly pandas_profiling

# -----------------------------------------------------------------------------
# Configure logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Profiling function
# -----------------------------------------------------------------------------
def profile_all_cities(
    raw_data_dir: str,
    month: str = "202409",
    cities: list = None,
    inline: bool = True,
    output_dir: str = None,
):
    """
    Generate a ProfileReport for each city's trip data.

    Parameters
    ----------
    raw_data_dir : str
        Root folder where raw_data/<City>/trip_data/<month>/<month>-combined.csv lives.
    month : str, default "202409"
        YYYYMM identifier for which month to profile.
    cities : list of str, optional
        List of city folder names. Defaults to ["NYC", "Chicago", "Boston", "Capital"].
    inline : bool, default True
        If True, calls `to_notebook_iframe()` for inline display in a Jupyter notebook.
    output_dir : str, optional
        If provided, saves each report as HTML under this directory.
    """
    if cities is None:
        cities = ["NYC", "Chicago", "Boston", "Capital"]

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    for city in cities:
        csv_path = os.path.join(
            raw_data_dir, city, "trip_data", month, f"{month}-combined.csv"
        )
        if not os.path.exists(csv_path):
            logger.warning(f"{city}: file not found at {csv_path}, skipping.")
            continue

        logger.info(f"{city}: loading data from {csv_path}")
        df = pd.read_csv(csv_path)

        logger.info(f"{city}: generating profile report")
        report = ProfileReport(
            df,
            title=f"{city} Bike Trip Data Quality Report",
            explorative=True,
        )

        if inline:
            report.to_notebook_iframe()

        if output_dir:
            out_file = os.path.join(output_dir, f"{city}_profile.html")
            report.to_file(out_file)
            logger.info(f"{city}: profile saved to {out_file}")

# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Determine this script's directory
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # Point at your raw_data folder (assumes ../raw_data relative to this script)
    raw_data_dir = os.path.join(script_dir, "..", "raw_data")

    # Optional: where to save the HTML reports
    output_dir = os.path.join(script_dir, "..", "bike_profiles")

    # Run profiling for September 2024 data, saving HTML files (no inline)
    profile_all_cities(
        raw_data_dir=raw_data_dir,
        month="202409",
        inline=False,
        output_dir=output_dir
    )
