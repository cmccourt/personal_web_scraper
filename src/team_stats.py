import traceback

from src.data_handlers.eihl_postgres import EIHLPostgresHandler
from src.match import get_eihl_match_url
from src.web_scraping.eihl_website_scraping import extract_team_match_stats


def insert_team_match_stats(data_src_hndlr: EIHLPostgresHandler, team_match_stats: dict):
    team_name = team_match_stats.get("team_name", None)
    match_id = team_match_stats.get("match_id", None)

    if data_src_hndlr.check_for_dups(params=team_match_stats, table="match_team_stats") or \
            data_src_hndlr.check_for_dups(params=team_match_stats, table="match_team_stats",
                                          where_clause="""match_id"=%(match_id)s AND "team_name"=%(team_name)s"""):
        try:
            data_src_hndlr.insert_data("match_team_stats", team_match_stats)
        except TypeError:
            traceback.print_exc()
        else:
            print(f"Match ID: {match_id} team: {team_name} stats inserted!")
    else:
        print(f"THERE ARE DUPLICATE records for team stats for match ID {match_id}, team: {team_name}.")


def update_match_stats(ds_handler: EIHLPostgresHandler, *matches: dict):
    try:
        for match in matches:
            match_url = get_eihl_match_url(match.get('eihl_web_match_id', ''))
            print(f"Next match is {match_url}")
            match_stats = extract_team_match_stats(match_url)
            update_match_team_stats(ds_handler, match_stats)
    except Exception:
        traceback.print_exc()


def update_match_team_stats(ds_handler: EIHLPostgresHandler, match_stats: dict):
    try:
        for k, team_stats in match_stats.items():
            # TODO data source handler should handle column name conversions
            team_stats["team_name"] = match_stats.get(k, None)
            team_stats["match_id"] = match_stats.get("match_id", None)
            insert_team_match_stats(ds_handler, team_stats)
    except Exception:
        traceback.print_exc()
