import traceback
from pprint import pprint

from settings.settings import eihl_schedule_url, eihl_match_url
from src.data_handlers.eihl_postgres import EIHLPostgresHandler
from src.web_scraping.eihl_website_scraping import get_matches, get_match_player_stats


def get_gamecentre_month_id(month_num: int = None) -> int:
    """

    Args:
        month_num: the month of the year in number form (e.g. January = 1, March = 3, October = 10 etc)

    Returns: month ID

    """
    if month_num is None:
        return 999
    elif month_num > 12:
        print(f"Month number {month_num} is Invalid")
    return month_num


# TODO create season ID converter in data source handler
def get_gamecentre_season_id(season: str):
    if season is None:
        return None
    else:
        return None


# TODO Create team season ID table in DB to hold team ID for each season
# TODO create team ID converter in data source handler
def get_gamecentre_team_id(team_name: str = None):
    if team_name is None:
        return 0
    else:
        return team_name


def get_gamecentre_url(team_id: int, month_id: int, season_id: int, base_url: str = eihl_schedule_url) -> str:
    season_url = f"{base_url}?id_season={season_id}&id_team={team_id}&id_month={month_id}"
    return season_url


def get_eihl_match_url(match_id: int, base_url: str = eihl_match_url) -> str:
    match_url = f"{base_url}{match_id}/team-stats"
    return match_url


def get_season_ids(data_source_hdlr, seasons: list[str]):
    season_ids = None
    if seasons is None:
        # get all EIHL season ids to iterate through
        season_ids = data_source_hdlr.fetchall("SELECT eihl_web_id FROM championship")
    return season_ids


def get_match_stats(match_stats_url):
    print(f"\nNext match is {match_stats_url}\n")
    match_stats = get_match_player_stats(match_stats_url)
    # Check if the team score table came through
    if len(match_stats) > 4 and len(match_stats[0].columns) <= 4:
        del match_stats[0]
    return match_stats


def insert_match(data_src_hndlr: EIHLPostgresHandler, match: dict):
    try:
        contains_dups = data_src_hndlr.check_for_dups(params=match, table="match")
        if contains_dups:
            # Find duplicates using datetime home team and away team only
            # Change where clause in case there is dup matches
            # TODO SQL INJECTION ALERT!
            where_clause = "\"match_date\"=%(match_date)s AND \"home_team\"=%(home_team)s AND \"away_team\"=%(away_team)s"
            contains_dups = data_src_hndlr.check_for_dups(params=match, table="match", where_clause=where_clause)

        if not contains_dups:
            data_src_hndlr.insert_data("match", match)
        else:
            print("Match already exists in DB")
    except Exception:
        traceback.print_exc()


def insert_championship(data_source_hdlr: EIHLPostgresHandler, champ: dict):
    try:
        if data_source_hdlr.check_for_dups(params=champ, table="championship"):
            data_source_hdlr.insert_data("championship", champ)
    except Exception:
        traceback.print_exc()
    else:
        print("Championship has been inserted!")


def insert_all_eihl_matches(data_source_hdlr, seasons: list[int] = None, gc_team_id: int = None,
                            gc_month_id: int = None):
    if gc_team_id is None:
        gc_team_id = get_gamecentre_team_id()
    if gc_month_id is None:
        gc_month_id = get_gamecentre_month_id()
    if seasons is None:
        # get all EIHL season ids to iterate through
        seasons = get_season_ids(data_source_hdlr)
    try:
        for season in seasons:
            season_url = get_gamecentre_url(gc_team_id, gc_month_id, season)
            season_matches = get_matches(season_url)
            for match in season_matches:
                pprint(match)
                # TODO should the data storage class handle column name conversions?
                match.update({"championship_id": season})
                insert_match(data_source_hdlr, match)
    except Exception:
        traceback.print_exc()
