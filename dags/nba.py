"""NBA data scraping DAG that collects player and team statistics from the NBA API."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from typing import Dict, Any, List, cast
from include import nba_api_client
from include import gcs

default_args: Dict[str, Any] = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def scrape_nba_data() -> str:
    """Scrape NBA data from the official API and upload to GCS."""
    # Get all teams
    teams_df: pd.DataFrame = nba_api_client.get_all_teams()

    all_players_data: List[Dict[str, Any]] = []
    all_stats_data: List[Dict[str, Any]] = []

    # For each team, get their roster and player stats
    for _, team in teams_df.iterrows():
        team_roster: pd.DataFrame = nba_api_client.get_team_roster(team["id"])

        for _, player in team_roster.iterrows():
            player_stats: pd.DataFrame = nba_api_client.get_player_stats(
                player["PERSON_ID"]
            )

            if not player_stats.empty:
                # Add team information to player data
                player_data: Dict[str, Any] = cast(Dict[str, Any], player.to_dict())
                player_data["TEAM_NAME"] = team["full_name"]
                all_players_data.append(player_data)

                # Add player information to stats
                stats_data: Dict[str, Any] = cast(
                    Dict[str, Any], player_stats.to_dict("records")[0]
                )
                stats_data["TEAM_NAME"] = team["full_name"]
                all_stats_data.append(stats_data)

    # Convert to DataFrames
    players_df: pd.DataFrame = pd.DataFrame(all_players_data)
    stats_df: pd.DataFrame = pd.DataFrame(all_stats_data)

    # Get current date for the file path
    current_date: str = datetime.now().strftime("%Y%m%d")

    # Upload to GCS in Parquet format
    bucket_name: str = "pds-data-lake"
    gcs.upload_to_gcs(
        players_df, bucket_name, f"dev/nba/players_{current_date}.parquet"
    )
    gcs.upload_to_gcs(stats_df, bucket_name, f"dev/nba/stats_{current_date}.parquet")

    return "NBA data scraping completed successfully!"


with DAG(
    "nba",
    default_args=default_args,
    description="Scrapes NBA statistics using nba_api",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 4, 8),
    catchup=False,
    tags=["nba", "scraping"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_nba_data",
        python_callable=scrape_nba_data,
    )
