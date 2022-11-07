import traceback

import pandas as pd
import requests
from bs4 import BeautifulSoup

from settings.settings import eihl_team_url, eihl_match_url
from src.eihl_stats_db import db_connection, db_cursor, insert_team_match_stats, insert_player_match_stats
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
    except Exception:
        traceback.print_exc()
    finally:
        if db_conn is not None:
            if match_db_cursor is not None:
                match_db_cursor.close()
            if db_cur is not None:
                db_cur.close()
            db_conn.close()


def get_all_players_stats(url: str = None, team_stats: pd.DataFrame = None):
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

            match_stats_url = f"{eihl_match_url}{match.get('eihl_web_match_id', '')}/stats"
            print(f"\nNext match is {match_stats_url}\n")
            match_stats = get_page_stats(match_stats_url)
            # Check if the team score table came through
            if len(match_stats) > 4 and len(match_stats[0].columns) <= 4:
                del match_stats[0]

            for k, team_stats in match_stats.items():
                try:
                    team_stat_dicts = team_stats.to_dict('records')
                except AttributeError:
                    traceback.print_exc()
                else:
                    for player_stats in team_stat_dicts:
                        player_stats["team_name"] = k
                        player_stats["match_id"] = match.get("match_id", None)
                        insert_player_match_stats(player_stats, db_cur, db_conn)
                #team_stats.apply(insert_team_match_stats, args=(db_cur, db_conn, True), axis=1)
    except Exception:
        traceback.print_exc()
    else:
        print("Player match stats insertion Successful!!!")
    finally:
        if db_conn is not None:
            if match_db_cursor is not None:
                match_db_cursor.close()
            if db_cur is not None:
                db_cur.close()
            db_conn.close()
        print("COMPLETE")


get_all_players_stats()
