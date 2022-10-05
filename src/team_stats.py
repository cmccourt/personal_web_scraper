from dataclasses import dataclass
from datetime import datetime

import pandas as pd


@dataclass
class Player:
    name: str
    country: str
    dob: datetime
    stats: pd.DataFrame


@dataclass
class Team:
    name: str
    stats: pd.DataFrame
    players: [Player]


def get_team_stats(team_name: str):
    pass


def get_players_stats(player_name: str, team_stats: pd.DataFrame = None):
    pass
