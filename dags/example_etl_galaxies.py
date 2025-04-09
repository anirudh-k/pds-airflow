"""
## Galaxies ETL example DAG.

This example demonstrates an ETL pipeline using Airflow.
The pipeline mocks data extraction for data about galaxies using a modularized
function, filters the data based on the distance from the Milky Way, and loads the
filtered data into a DuckDB database.
"""

from airflow.decorators import (
    dag,
    task,
)  # TaskFlow API: https://www.astronomer.io/docs/learn/airflow-decorators
from airflow.models.dataset import Dataset
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from pendulum import datetime, duration, timedelta
from tabulate import tabulate
import pandas as pd
import duckdb
import logging
import os
from typing import Dict, Any, List, Union, cast

# modularize code by importing functions from the include folder
from include.custom_functions.galaxy_functions import (
    get_galaxy_data,
    get_connection,
    calculate_galaxy_properties,
    format_galaxy_data,
    create_galaxy_table_in_duckdb,
)

# use the Airflow task logger to log information to the task logs (or use print())
t_log = logging.getLogger("airflow.task")

# define variables used in a DAG as environment variables in .env
# for your whole Airflow instance to standardize your DAGs
_DUCKDB_INSTANCE_NAME = os.getenv("DUCKDB_INSTANCE_NAME", "include/astronomy.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "galaxy_data")
_DUCKDB_TABLE_URI = f"duckdb://{_DUCKDB_INSTANCE_NAME}/{_DUCKDB_TABLE_NAME}"
_CLOSENESS_THRESHOLD_LY_DEFAULT = os.getenv("CLOSENESS_THRESHOLD_LY_DEFAULT", "500000")
_CLOSENESS_THRESHOLD_LY_PARAMETER_NAME = "closeness_threshold_light_years"
_NUM_GALAXIES_TOTAL = os.getenv("NUM_GALAXIES_TOTAL", "20")


# -------------- #
# DAG Definition #
# -------------- #

# Define datasets
galaxy_data = Dataset("file://include/data/galaxies.json")
galaxy_properties = Dataset("file://include/data/galaxy_properties.json")


# instantiate a DAG with the @dag decorator and set DAG parameters
@dag(
    dag_id="example_etl_galaxies",
    default_args={
        "owner": "Astro",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(seconds=30),  # tasks wait 30s in between retries
    },
    description="Example ETL DAG for working with galaxy data",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 4, 8),
    catchup=False,
    tags=["example", "etl", "galaxies"],
    params={
        "num_galaxies": Param(
            10,
            type=["string", "integer"],
            minimum=1,
            maximum=100,
            description="Number of galaxies to process",
        )
    },
)
def galaxy_etl() -> None:
    """Run the main ETL pipeline for galaxy data."""

    @task(outlets=[galaxy_data])
    def extract_galaxy_data(num_galaxies: Union[str, int]) -> List[Dict[str, Any]]:
        """Extract galaxy data from the SQLite database."""
        # Get connection
        conn = get_connection()

        # Get data
        galaxies = get_galaxy_data(conn, int(num_galaxies))

        # Close connection
        conn.close()

        return galaxies

    @task(outlets=[galaxy_properties])
    def transform_galaxy_data(galaxies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform galaxy data by calculating additional properties."""
        # Calculate properties for each galaxy
        galaxies_with_properties = []
        for galaxy in galaxies:
            properties = calculate_galaxy_properties(galaxy)
            formatted_galaxy = format_galaxy_data(properties)
            galaxies_with_properties.append(formatted_galaxy)

        return galaxies_with_properties

    @task
    def load_galaxy_data(galaxies: List[Dict[str, Any]]) -> None:
        """Load galaxy data into DuckDB."""
        # Create table if it doesn't exist
        create_galaxy_table_in_duckdb()

        # Connect to DuckDB
        con = duckdb.connect(database=":memory:", read_only=False)

        # Convert to DataFrame and register it with DuckDB
        galaxies_df = pd.DataFrame(galaxies)
        con.register("galaxies_temp", galaxies_df)
        con.execute("INSERT INTO galaxies SELECT * FROM galaxies_temp")

        # Close connection
        con.close()

    @task
    def print_loaded_galaxies() -> None:
        """Print the loaded galaxies from DuckDB."""
        # Connect to DuckDB
        con = duckdb.connect(database=":memory:", read_only=True)

        # Query galaxies
        result_df = con.execute("SELECT * FROM galaxies").fetchdf()

        # Print table
        print("\nLoaded Galaxies:")
        print(
            tabulate(
                cast(List[Dict[str, Any]], result_df.to_dict("records")),
                headers="keys",
                tablefmt="psql",
            )
        )

        # Close connection
        con.close()

    # Get parameters
    num_galaxies = "{{ params.num_galaxies }}"

    # Define tasks
    extract = extract_galaxy_data(num_galaxies)
    transform = transform_galaxy_data(extract)
    load = load_galaxy_data(transform)
    print_galaxies = print_loaded_galaxies()

    # Set dependencies
    chain(extract, transform, load, print_galaxies)


# Instantiate the DAG
galaxy_etl()
