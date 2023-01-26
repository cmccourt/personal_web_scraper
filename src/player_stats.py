import traceback

import pandas as pd

from settings.settings import eihl_match_url
# TODO Create Protocol for DB handler
from src.data_handlers.eihl_mysql import EIHLMysqlHandler
from src.web_scraping.eihl_website_scraping import get_match_stats


def insert_player_match_stats(db_handler: EIHLMysqlHandler, player_match_stats: dict):
    team_name = player_match_stats.get("team_name", None)
    player_name = player_match_stats.get("player_name", None)
    match_id = player_match_stats.get("match_id", None)
    # TODO find better solution to replace NaNs with Nones
    player_match_stats = {k: (v if not isinstance(v, float) or not pd.isna(v) else None)
                          for k, v in player_match_stats.items()}

    try:
        if db_handler.check_for_dups(params=player_match_stats, table="match_player_stats"):
            print(f"Match ID: {match_id}, team: {team_name}, player {player_name} stats to be inserted!")
            db_handler.insert_data("match_player_stats", player_match_stats)
        else:
            print(f"Match ID {match_id} team: {team_name}, player: {player_name} stats already exists in DB.")
    except Exception:
        traceback.print_exc()


def insert_all_players_stats(matches, db_handler: EIHLMysqlHandler):
    try:
        for match in matches:
            match_stats_url = f"{eihl_match_url}{match.get('eihl_web_match_id', '')}/stats"
            match_stats: pd.DataFrame = get_match_stats(match_stats_url)
            match_stats = match_stats.rename(columns=db_handler.match_player_stats_cols)

            for k, team_stats in match_stats.items():
                try:
                    team_stat_dicts = team_stats.to_dict('records')
                except AttributeError:
                    traceback.print_exc()
                else:
                    for player_stats in team_stat_dicts:
                        player_stats["team_name"] = k
                        player_stats["match_id"] = match.get("match_id", None)
                        insert_player_match_stats(db_handler, player_stats)
                # team_stats.apply(insert_team_match_stats, args=(db_cur, db_conn, True), axis=1)
    except Exception:
        traceback.print_exc()
    else:
        print("Player match stats insertion Successful!!!")
