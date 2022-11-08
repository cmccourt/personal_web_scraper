import traceback
import pandas as pd

from settings.settings import eihl_match_url
from src.eihl_stats_db import insert_team_match_stats, insert_player_match_stats, get_next_match
from webScraping import get_page_stats, get_team_match_stats


def get_match_team_stats():
    try:
        while True:
            match = get_next_match()
            if match is None:
                break
            match_url = f"{eihl_match_url}{match.get('eihl_web_match_id', '')}/team-stats"
            print(f"Next match is {match_url}")
            match_stats = get_team_match_stats(match_url)
            for k, team_stats in match_stats.items():
                team_stats["team_name"] = match.get(k, None)
                team_stats["match_id"] = match.get("match_id", None)

                insert_team_match_stats(team_stats, update_exist_data=True)
    except Exception:
        traceback.print_exc()


def get_all_players_stats():
    try:
        while True:
            match = get_next_match()
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
                        insert_player_match_stats(player_stats)
                # team_stats.apply(insert_team_match_stats, args=(db_cur, db_conn, True), axis=1)
    except Exception:
        traceback.print_exc()
    else:
        print("Player match stats insertion Successful!!!")

