import traceback
from datetime import datetime
from pprint import pprint

# TODO Create Protocol for DB handler
from src.data_handlers.eihl_mysql import EIHLMysqlHandler
from src.web_scraping.eihl_website_scraping import get_matches, get_gamecentre_month_id, \
    get_gamecentre_team_id, get_gamecentre_url, get_eihl_championship_options


# TODO Create team season ID table in DB to hold team ID for each season
# TODO create team ID converter in data source handler
def get_db_season_ids(db_handler: EIHLMysqlHandler, seasons: list[str] = None):
    season_ids = None
    if seasons is None:
        # get all EIHL season ids to iterate through
        season_ids = db_handler.fetch_all_data("SELECT eihl_web_id FROM championship")
    return season_ids


def get_match_stats():
    pass


def insert_match_to_db(db_handler: EIHLMysqlHandler, match: dict):
    try:
        contains_dups = db_handler.check_for_dups(params=match, table="match")
        if contains_dups:
            # Find duplicates using datetime home team and away team only
            # Change where clause in case there is dup matches
            # TODO SQL INJECTION ALERT!
            where_clause = "match_date=%(match_date)s AND home_team=%(home_team)s AND away_team=%(away_team)s"
            contains_dups = db_handler.check_for_dups(params=match, table="match", where_clause=where_clause)

        if not contains_dups:
            db_handler.insert_data("match", match)
        else:
            print("Match already exists in DB")
    except Exception:
        traceback.print_exc()


def insert_championship_to_db(data_source_hdlr: EIHLMysqlHandler, *championships: dict):
    for champ in championships:
        try:
            if not data_source_hdlr.check_for_dups(params=champ, table="championship"):
                data_source_hdlr.insert_data("championship", champ)
        except Exception:
            traceback.print_exc()
        else:
            print("Championship has been inserted!")


def get_db_matches(db_handler: EIHLMysqlHandler, teams: list[str] = None,
                   start_date: datetime = None, end_date: datetime = None):
    if not start_date:
        start_date = datetime.min
    if not end_date:
        end_date = datetime.max
    if not teams:
        matches = db_handler.fetch_all_data("SELECT * FROM `match` WHERE match_date BETWEEN %(start_date)s "
                                            "AND %(end_date)s",
                                            {"start_date": start_date, "end_date": end_date})
    else:
        matches = db_handler.fetch_all_data("SELECT * FROM `match` WHERE match_date BETWEEN %(start_date)s "
                                            "AND %(end_date)s AND (home_team IN %(teams)s OR away_team IN %(teams)s)",
                                            {"teams": teams, "start_date": start_date, "end_date": end_date})
    return matches


def insert_all_eihl_matches_to_db(db_handler, num_threads=4):
    gc_team_id = get_gamecentre_team_id()
    gc_month_id = get_gamecentre_month_id()
    seasons = get_db_season_ids(db_handler)
    if seasons is None or len(seasons) == 0:
        seasons = get_eihl_championship_options()
        insert_championship_to_db(db_handler, *seasons)

    try:
        for season in seasons:
            season_id = season["eihl_web_id"]
            season_gamecentre_url = get_gamecentre_url(gc_team_id, gc_month_id, season_id)
            season_matches = get_matches(season_gamecentre_url)
            for match in season_matches:
                pprint(match)
                # TODO should the data storage class handle column name conversions?
                match.update({"championship_id": season_id})
                insert_match_to_db(db_handler, match)
    except Exception:
        traceback.print_exc()
