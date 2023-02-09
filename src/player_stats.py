import traceback
from datetime import datetime
from queue import Queue
from threading import Thread
from typing import Callable

import pandas as pd

from settings.settings import eihl_match_url
# TODO Create Protocol for DB handler
from src.data_handlers.eihl_mysql import EIHLMysqlHandler
from src.match import get_db_matches
from src.web_scraping.eihl_website_scraping import extract_match_stats


def build_match_stats_url(eihl_web_match_id):
    match_stats_url = f"{eihl_match_url}{eihl_web_match_id}/stats"
    return match_stats_url


def player_stats_producer(stat_queue, matches):
    try:
        print("Producer: Running")
        for match_info in matches:
            match_stats_url = build_match_stats_url(match_info.get("eihl_web_match_id", ""))
            stat_queue.put((match_info, match_stats_url))

        print("Producer: Done")
        # team_stats.apply(insert_team_match_stats, args=(db_cur, db_conn, True), axis=1)
    except Exception:
        traceback.print_exc()


def player_stats_consumer(stat_queue, db_object_func):
    print("Consumer: Running")
    db_handler = db_object_func()
    while True:
        match_info, match_url = stat_queue.get(block=True)
        if match_info is None:
            break
        try:
            all_player_stats = get_player_stats(db_handler, match=match_info, match_stats_url=match_url)
            for team_player_stats in all_player_stats:
                insert_player_match_stats(db_handler, *team_player_stats)
        except Exception:
            traceback.print_exc()
        finally:
            stat_queue.task_done()
    print('Consumer: Done')


def insert_player_match_stats(db_handler: EIHLMysqlHandler, *player_match_stats: dict):
    for player_stats in player_match_stats:
        team_name = player_stats.get("team_name", None)
        player_name = player_stats.get("player_name", None)
        match_id = player_stats.get("match_id", None)
        # TODO find better solution to replace NaNs with Nones
        player_stats = {k: (v if not isinstance(v, float) or not pd.isna(v) else None)
                        for k, v in player_stats.items()}

        try:
            if len(db_handler.get_dup_records(params=player_stats, table="match_player_stats")) == 0:
                print(f"Match ID: {match_id}, team: {team_name}, player {player_name} stats to be inserted!")
                db_handler.insert_data("match_player_stats", player_stats)
            else:
                print(f"Match ID {match_id} team: {team_name}, player: {player_name} stats already exists in DB.")
        except Exception:
            traceback.print_exc()


def get_player_stats(db_handler, match=None, match_stats_url: str = None) -> list[dict]:
    player_stats = []
    match_stats: pd.DataFrame = extract_match_stats(match_stats_url)
    for team_name, team_stats in match_stats.items():
        team_stats = team_stats.rename(columns=db_handler.match_player_stats_cols)
        try:
            team_stats["team_name"] = team_name
            team_stats["match_id"] = match.get("match_id", None)
            team_stat_dicts: dict = team_stats.to_dict('records')
            player_stats.append(team_stat_dicts)
        except AttributeError:
            traceback.print_exc()
            return
    return player_stats


def insert_all_players_stats(db_handler: EIHLMysqlHandler):
    matches = get_db_matches(db_handler)
    try:
        for match in matches:
            match_stats_url = build_match_stats_url(match.get("eihl_web_match_id", ""))
            all_player_stats = get_player_stats(db_handler, match, match_stats_url)
            for team_player_stats in all_player_stats:
                insert_player_match_stats(db_handler, *team_player_stats)
        # team_stats.apply(insert_team_match_stats, args=(db_cur, db_conn, True), axis=1)
    except Exception:
        traceback.print_exc()
    else:
        print("Player match stats insertion Successful!!!")


def insert_all_players_stats_concurrently(db_obj_func: Callable, matches: list[dict] = None, num_threads=5):
    db_handler = db_obj_func()
    if matches is None or len(matches) == 0:
        matches = get_db_matches(db_handler, end_date=datetime.now())

    matches_queue = Queue()
    player_stats_producer(matches_queue, matches)
    consumers = [Thread(target=player_stats_consumer, args=(matches_queue, db_obj_func,)) for i in range(num_threads)]
    # TODO implement logging
    try:
        for consumer in consumers:
            # Setting daemon to True will let the main thread exit even though the workers are blocking
            consumer.daemon = True
            consumer.start()
    except Exception:
        print("THREADING ERROR!")
    # producer = Thread(target=player_stats_producer, args=(matches_queue, matches))
    # producer.start()
    # producer.join()
    # for consumer in consumers:
    #     consumer.join()
    matches_queue.join()
    print("Player match stats insertion Successful!!!")
