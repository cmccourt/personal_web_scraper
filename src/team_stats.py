import traceback
from datetime import datetime
from queue import Queue
from threading import Thread

from pypika import Parameter, Field

# TODO Create Protocol for DB handler
from src.data_handlers.eihl_mysql import EIHLMysqlHandler
from src.match import get_db_matches
from src.web_scraping.eihl_website_scraping import extract_team_match_stats, get_eihl_match_url


def team_stats_producer(stat_queue, matches):
    try:
        print("Producer: Running")
        for match_info in matches:
            match_stats_url = get_eihl_match_url(match_info.get('eihl_web_match_id', ''))
            stat_queue.put((match_info, match_stats_url))
        print("Producer: Done")
        # team_stats.apply(insert_team_match_stats, args=(db_cur, db_conn, True), axis=1)
    except Exception:
        traceback.print_exc()


def team_stats_consumer(stat_queue, db_object_func):
    print("Consumer: Running")
    db_handler = db_object_func()
    while True:
        match_info, match_url = stat_queue.get(block=True)
        if match_info is None:
            break
        try:
            match_stats = extract_team_match_stats(match_url)
            for k, team_stats in match_stats.items():
                # TODO data source handler should handle column name conversions
                team_stats["team_name"] = match_info.get(k, None)
                team_stats["match_id"] = match_info.get("match_id", None)
                team_stats = {db_handler.match_team_stats_cols.get(k, k): v for k, v in team_stats.items()}
                insert_team_match_stats_to_db(db_handler, team_stats)
        except Exception:
            traceback.print_exc()
        finally:
            stat_queue.task_done()
    print('Consumer: Done')


def insert_team_match_stats_to_db(db_handler: EIHLMysqlHandler, team_match_stats: dict):
    team_name = team_match_stats.get("team_name", None)
    match_id = team_match_stats.get("match_id", None)
    dup_records = db_handler.get_dup_records(params=team_match_stats, table="match_team_stats",
                                             where_clause=((Field("match_id") == Parameter("%(match_id)s")) &
                                                           (Field("team_name") == Parameter("%(team_name)s"))
                                                           )
                                             )

    if len(dup_records) == 0:
        try:
            db_handler.insert_data("match_team_stats", team_match_stats)
        except TypeError:
            traceback.print_exc()
        else:
            print(f"Match ID: {match_id} team: {team_name} stats inserted!")
    else:
        db_handler.update_data("match_team_stats", team_match_stats,
                               where_clause=((Field("match_id") == Parameter("%(match_id)s")) &
                                             (Field("team_name") == Parameter("%(team_name)s"))
                                             )
                               )


def update_match_team_stats(db_obj_func: callable, num_threads=5, matches: list[dict] = None):
    db_handler = db_obj_func()
    if len(matches) == 0:
        matches = get_db_matches(db_handler, end_date=datetime.now())

    matches_queue = Queue()
    team_stats_producer(matches_queue, matches)
    consumers = [Thread(target=team_stats_consumer, args=(matches_queue, db_obj_func,)) for i in range(num_threads)]
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
    print("Team match stats insertion Successful!!!")
