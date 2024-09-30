import traceback
from datetime import datetime
from queue import Queue
from threading import Thread

import pandas as pd

from src.data_handlers.eihl_mysql import get_dup_records, insert_data, update_data, match_player_stats_cols
from src.match import get_db_matches
from src.web_scraping.website import Website


def insert_player_stats_to_db(*player_match_stats: dict):
    for player_stats in player_match_stats:
        team_name = player_stats.get("team_name", None)
        player_name = player_stats.get("player_name", None)
        match_id = player_stats.get("match_id", None)
        # TODO find better solution to replace NaNs with Nones
        player_stats = {k: (v if not isinstance(v, float) or not pd.isna(v) else None)
                        for k, v in player_stats.items()}

        try:
            if len(get_dup_records(params=player_stats, table="match_player_stats")) == 0:
                print(f"Match ID: {match_id}, team: {team_name}, player {player_name} stats to be inserted!")
                insert_data("match_player_stats", player_stats)
            else:
                print(f"Match ID {match_id} team: {team_name}, player: {player_name} stats already exists in DB.")
                update_data("match_player_stats", player_stats)
                print(f"Match ID {match_id} team: {team_name}, player: {player_name} Updated Successfully.")
        except Exception:
            traceback.print_exc()


def get_player_stats(website: Website, match=None, match_stats_url: str = None) -> list[dict]:
    player_stats = []
    match_stats: pd.DataFrame = website.extract_match_info(match_stats_url)
    for team_name, team_stats in match_stats.items():
        team_stats = team_stats.rename(columns=match_player_stats_cols)
        try:
            team_stats["team_name"] = team_name
            team_stats["match_id"] = match.get("match_id", None)
            team_stat_dicts: dict = team_stats.to_dict('records')
            player_stats.append(team_stat_dicts)
        except AttributeError:
            traceback.print_exc()
    return player_stats


def player_stats_producer(stat_queue, matches, website: Website):
    try:
        print("Producer: Running")
        for match_info in matches:
            match_stats_url = website.get_match_stats_url()
            # build_match_stats_url(match_info.get("eihl_web_match_id", ""))
            stat_queue.put((match_info, match_stats_url))

        print("Producer: Done")
        # team_stats.apply(insert_team_match_stats, args=(db_cur, db_conn, True), axis=1)
    except Exception:
        traceback.print_exc()


def player_stats_consumer(stat_queue):
    print("Consumer: Running")
    while True:
        match_info, match_url = stat_queue.get(block=True)
        if match_info is None:
            break
        try:
            all_player_stats = get_player_stats(match=match_info, match_stats_url=match_url)
            for team_player_stats in all_player_stats:
                insert_player_stats_to_db(*team_player_stats)
        except Exception:
            traceback.print_exc()
        finally:
            stat_queue.task_done()
    print('Consumer: Done')


def update_players_stats_to_db(matches: list[dict] = None, num_threads=5):
    if matches is None or len(matches) == 0:
        matches = get_db_matches(end_date=datetime.now())

    matches_queue = Queue()
    player_stats_producer(matches_queue, matches)
    consumers = [Thread(target=player_stats_consumer, args=(matches_queue,)) for i in range(num_threads)]
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
