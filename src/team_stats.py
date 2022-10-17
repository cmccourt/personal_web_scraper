import re
from collections import defaultdict
from datetime import datetime
from typing import Callable

import pandas as pd
import requests
from bs4 import BeautifulSoup
from settings.settings import eihl_team_url, eihl_match_url
from src.eihl_stats_db import db_connection, insert_match, db_cursor, insert_championship
from webScraping import get_page_stats, get_html_content, get_matches, get_eihl_championship_options

"""@dataclass
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
"""


def get_team_stats(team_name: str):
    # "https://www.eliteleague.co.uk/team/4-belfast-giants/player-stats?id_season=37"

    team_url = f"{eihl_team_url}4-belfast-giants/player-stats?id_season=37"
    response = requests.get(team_url)
    res_beaus = BeautifulSoup(response.content, 'html.parser')

    team_stats = get_page_stats(team_url)
    return team_stats


def get_players_stats(player_name: str, team_stats: pd.DataFrame = None):
    pass


# eihl_matches = get_matches("https://www.eliteleague.co.uk/schedule?id_season=36&id_team=0&id_month=999", lambda x: True)
db_conn = None
db_cur = None
try:
    db_conn = db_connection()
    db_cur = db_cursor(db_conn)
    champion_options = get_eihl_championship_options()
    print("COMPLETE")
    for match in champion_options:
        insert_championship(match, db_cur, db_conn)
except Exception as e:
    print(f"ERROR! {e}")
finally:
    if db_conn is not None:
        if db_cur is not None:
            db_cur.close()
        db_conn.close()
