import traceback

import pandas as pd
import requests
from bs4 import BeautifulSoup

from settings.settings import eihl_team_url, eihl_match_url
from src.eihl_stats_db import db_connection, db_cursor, insert_team_match_stats
from webScraping import get_page_stats, populate_all_eihl_matches, get_team_match_stats

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


def get_match_team_stats():
    db_conn = None
    db_cur = None
    match_db_cursor = None
    try:
        db_conn = db_connection()
        db_cur = db_cursor(db_conn)
        # TODO find a better solution to handle DB connections and cursors
        match_db_cursor = db_cursor(db_conn)
        match_db_cursor.execute("SELECT * FROM match;")
        while True:
            match = match_db_cursor.fetchone()
            if match is None:
                break

            match_url = f"{eihl_match_url}{match.get('eihl_web_match_id', '')}/team-stats"
            print(f"Next match is {match_url}")
            match_stats = get_team_match_stats(match_url)
            # TODO insert match stats to match_team_stats table
            for k, team_stats in match_stats.items():
                team_stats["team_name"] = match.get(k, None)
                team_stats["match_id"] = match.get("match_id", None)
                insert_team_match_stats(team_stats, db_cur, db_conn, True)
            print(match_stats)

        print("COMPLETE")

    except Exception:
        traceback.print_exc()
    finally:
        if db_conn is not None:
            if match_db_cursor is not None:
                match_db_cursor.close()
            if db_cur is not None:
                db_cur.close()
            db_conn.close()


def get_players_stats(player_name: str, team_stats: pd.DataFrame = None):
    pass




