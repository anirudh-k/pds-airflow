"""
NBA API client utilities.

This module provides functions for interacting with the NBA API,
including retrieving team information, player rosters, and player statistics.
"""

from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonallplayers, leaguedashplayerstats
import pandas as pd
import time
from typing import List, Dict, Any


def get_all_teams() -> pd.DataFrame:
    """Get all NBA teams and their basic information."""
    all_teams: List[Dict[str, Any]] = teams.get_teams()
    return pd.DataFrame(all_teams)


def get_team_roster(team_id: int) -> pd.DataFrame:
    """Get the current roster for a specific team."""
    # Add a small delay to avoid rate limiting
    time.sleep(0.6)
    players: pd.DataFrame = commonallplayers.CommonAllPlayers(
        league_id="00", season="2023-24", is_only_current_season=1
    ).get_data_frames()[0]

    # Filter for players on the specific team
    team_players: pd.DataFrame = players[players["TEAM_ID"] == team_id]
    return team_players


def get_player_stats(player_id: int) -> pd.DataFrame:
    """Get career statistics for a specific player."""
    # Add a small delay to avoid rate limiting
    time.sleep(0.6)
    stats: pd.DataFrame = leaguedashplayerstats.LeagueDashPlayerStats(
        season="2023-24",
        season_type_all_star="Regular Season",
        measure_type_detailed_defense="Base",
        per_mode_detailed="PerGame",
    ).get_data_frames()[0]

    # Filter for the specific player
    player_stats: pd.DataFrame = stats[stats["PLAYER_ID"] == player_id]
    return player_stats
