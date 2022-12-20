import traceback

from src.eihl_stats_db import EIHLDBHandler
from src.match import get_eihl_match_url
from webScraping import extract_team_match_stats


def insert_team_match_stats(data_src_hndlr: EIHLDBHandler, team_match_stats: dict):
    team_name = team_match_stats.get("team_name", None)
    match_id = team_match_stats.get("match_id", None)

    # where_clause = generate_and_where_clause(team_match_stats)
    # dup_match_sql = sql.SQL(
    #     """SELECT * FROM match_team_stats WHERE ({}) OR match_id"=%(match_id)s AND "team_name"=%(team_name)s""".format(
    #         data_src_hndlr.as_string(where_clause)
    #     ))
    # dup_matches = data_src_hndlr.fetch_all_data(dup_match_sql, team_match_stats)

    if data_src_hndlr.check_for_dups(params=team_match_stats, table="match_team_stats"):
        try:
            data_src_hndlr.insert_data("match_team_stats", team_match_stats)
        except TypeError:
            traceback.print_exc()
        else:
            print(f"Match ID: {match_id} team: {team_name} stats inserted!")
    else:
        print(f"THERE ARE DUPLICATE records for team stats for match ID {match_id}, team: {team_name}.")


def update_match_stats(ds_handler: EIHLDBHandler, *matches: dict):
    try:
        for match in matches:
            match_url = get_eihl_match_url(match.get('eihl_web_match_id', ''))
            print(f"Next match is {match_url}")
            match_stats = extract_team_match_stats(match_url)
            update_match_team_stats(ds_handler, match_stats)
    except Exception:
        traceback.print_exc()


def update_match_team_stats(ds_handler: EIHLDBHandler, match_stats: dict):
    try:
        for k, team_stats in match_stats.items():
            # TODO data source handler should handle column name conversions
            team_stats["team_name"] = match_stats.get(k, None)
            team_stats["match_id"] = match_stats.get("match_id", None)
            ds_handler.insert_team_match_stats(team_stats, update_exist_data=True)
    except Exception:
        traceback.print_exc()
