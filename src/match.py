import traceback
from datetime import datetime
from pprint import pprint

from pypika import Query, Field, Parameter

# TODO Create Protocol for DB handler
from src.data_handlers.eihl_mysql import EIHLMysqlHandler
from src.web_scraping.eihl_website_scraping import get_matches_from_web_gamecentre, get_gamecentre_url, \
    get_eihl_championship_options, get_start_end_dates_from_gamecentre, \
    get_match_info_from_match_page


# TODO Create team season ID table in DB to hold team ID for each season
# TODO create team ID converter in data source handler
def get_db_season_ids(db_handler: EIHLMysqlHandler, seasons: list[str] = None):
    season_ids = None
    if seasons is None:
        # get all EIHL season ids to iterate through
        season_ids = db_handler.fetch_all_data(Query.from_("championship").select("eihl_web_id"))
    return season_ids


def update_db_match(db_handler, match: dict, where_clause=None):
    try:
        db_handler.update_data("match", match, where_clause)
    except Exception:
        traceback.print_exc()


def insert_match_to_db(db_handler: EIHLMysqlHandler, match: dict):
    try:
        db_handler.insert_data("match", match)
    except Exception:
        traceback.print_exc()


def insert_championship_to_db(data_source_hdlr: EIHLMysqlHandler, *championships: dict):
    for champ in championships:
        try:
            if len(data_source_hdlr.get_dup_records(params=champ, table="championship")) == 0:
                data_source_hdlr.insert_data("championship", champ)
        except Exception:
            traceback.print_exc()
        else:
            print("Championship has been inserted!")


def refresh_championships(db_handler: EIHLMysqlHandler):
    db_champs = db_handler.fetch_all_data(Query.from_("championship").select(Field("eihl_web_id"), Field("name")))
    eihl_web_champs = get_eihl_championship_options()
    if eihl_web_champs == db_champs:
        print(f"All championship options are stored in the database")
    else:
        champ_diff = [x for x in eihl_web_champs if x not in db_champs]
        for champ in champ_diff:
            champ_schedule_url = get_gamecentre_url(season_id=champ.get("eihl_web_id", None))
            start_dt, end_dt = get_start_end_dates_from_gamecentre(champ_schedule_url)
            champ["start_date"] = start_dt
            champ["end_date"] = end_dt
        insert_championship_to_db(db_handler, *champ_diff)


def get_db_matches(db_handler: EIHLMysqlHandler, teams: list[str] = None,
                   start_date: datetime = None, end_date: datetime = None):
    if not start_date:
        start_date = datetime.min
    if not end_date:
        end_date = datetime.max
    match_query = Query.from_("match").select("*").where((Field("match_date").between(start_date, end_date)))
    if teams:
        match_query = match_query.where((Field("home_team").isin(teams)) | (Field("away_team").isin(teams)))

    matches = db_handler.fetch_all_data(str(match_query))
    return matches


def update_match_scores(db_handler: EIHLMysqlHandler, match_urls):
    dup_clause = ((Field("match_date") == Parameter("%(match_date)s")) &
                  (Field("home_team") == Parameter("%(home_team)s")) &
                  (Field("away_team") == Parameter("%(away_team)s")))
    for match_url in match_urls:
        match_info = get_match_info_from_match_page(match_url)
        dup_records = db_handler.get_dup_records(match_info, table="match", where_clause=dup_clause)

        if dup_records and \
                (dup_records[0].get("home_score", None) is None or dup_records[0].get("away_score", None) is None):
            db_handler.update_data("match", match_info, where_clause=dup_clause)
            print(f"Match Successfully updated: {match_url}")
        else:
            print(f"ERROR cannot find {match_info} in DB")


def update_eihl_scores_from_game_centre(db_handler, team_ids=None, month_ids=None, season_ids=None):
    dup_clause = ((Field("match_date") == Parameter("%(match_date)s")) &
                  (Field("home_team") == Parameter("%(home_team)s")) &
                  (Field("away_team") == Parameter("%(away_team)s")))

    try:
        for season in season_ids:
            season_id = season["eihl_web_id"]
            season_gamecentre_url = get_gamecentre_url(season_id, team_ids, month_ids)
            season_matches = get_matches_from_web_gamecentre(season_gamecentre_url)
            update_match_scores(db_handler, season_matches)
            for match in season_matches:
                pprint(match)
                # TODO should the data storage class handle column name conversions?
                match.update({"championship_id": season_id})

                dup_records = db_handler.get_dup_records(params=match, table="match", where_clause=dup_clause)
                if len(dup_records) > 1:
                    # TODO Implement logging for this scenario
                    print(f"More than 1 duplicate for match: {match}")
                elif len(dup_records) == 0:
                    insert_match_to_db(db_handler, match)
    except Exception:
        traceback.print_exc()
