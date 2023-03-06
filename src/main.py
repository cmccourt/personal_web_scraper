import dataclasses
from datetime import datetime
from enum import Enum
from typing import Callable, Tuple

from settings.settings import eihl_match_url
from src.data_handlers.eihl_mysql import EIHLMysqlHandler
# from src.data_handlers.eihl_postgres import EIHLPostgresHandler
from src.match import update_eihl_scores_from_game_centre, refresh_championships, get_db_season_ids, \
    update_match_scores
from src.player_stats import update_players_stats
from src.team_stats import update_match_team_stats
from src.web_scraping.eihl_website_scraping import get_gamecentre_team_id, get_eihl_championship_options, \
    get_gamecentre_month_id

# TODO make builder function to get data source handler
# ds_handler = EIHLPostgresHandler()
db_handler_func = EIHLMysqlHandler


@dataclasses.dataclass(init=True)
class CMDOption:
    help: str or None
    action: Callable
    params: Tuple or None = None


def display_help():
    print("Help\n")
    for option in Options:
        print(f"{option.name}: {option.value.help}")


def enter_date(fmt: str = "%d/%m/%Y"):
    while True:
        date_input = input("Enter date -> ")
        try:
            return datetime.strptime(date_input, fmt)
        except ValueError:
            # TODO find a way to display the format parameter in error message
            print("Invalid format! Format must be DD/MM/YY. Try again.")


def get_data_range() -> tuple[datetime, datetime]:
    while True:
        start_date = enter_date()
        end_date = enter_date()
        if start_date <= end_date:
            break
        print("Start date after end_date. Try again.")
    return start_date, end_date


def get_ids(team_names: list = None, months: list = None, seasons: list = None, db_handler=None):
    if db_handler is None:
        db_handler = db_handler_func()
    gc_team_id = get_gamecentre_team_id()
    gc_month_id = get_gamecentre_month_id()
    season_ids = get_db_season_ids(db_handler)
    if season_ids is None or len(season_ids) == 0:
        print("There are no championship IDs stored in the DB")
        season_ids = get_eihl_championship_options()
        # insert_championship_to_db(db_handler, *season_ids)
    return gc_team_id, gc_month_id, season_ids


def refresh_db():
    db_handler = db_handler_func()
    refresh_championships(db_handler)
    update_eihl_scores_from_game_centre(db_handler, True)
    update_match_team_stats(db_handler_func)
    update_players_stats(db_handler_func)


def update_recent_data():
    db_handler = db_handler_func()
    refresh_championships(db_handler)
    matches = db_handler.fetch_all_data("SELECT * FROM `match` WHERE home_score IS NULL or away_score IS NULL")
    if len(matches) > 0:
        match_urls = [f"{eihl_match_url}{match.get('eihl_web_match_id', None)}" for match in matches]
        update_match_scores(db_handler, match_urls)
        missing_team_stat_games = db_handler.fetch_all_data(
            """SELECT * FROM `match` WHERE match_id IN (SELECT m.match_id FROM `match` m LEFT JOIN match_team_stats mts ON m.match_id = mts.match_id 
GROUP BY m.match_id, mts.shots, mts.saves, m.home_score, m.away_score HAVING mts.shots=0 or mts.saves=0 OR m.home_score IS NULL or m.away_score IS NULL)""")
        update_match_team_stats(db_handler_func, matches=missing_team_stat_games)
        missing_player_stat_games = db_handler.fetch_all_data(
            """SELECT * FROM `match` WHERE match_id IN (SELECT m.match_id FROM `match` m LEFT JOIN match_player_stats mps ON mps.match_id = m.match_id 
GROUP BY m.match_id, mps.goals, mps.shutouts, m.home_score, m.away_score HAVING (mps.goals=0 AND mps.shutouts=0) OR m.home_score IS NULL or m.away_score IS NULL)""")
        update_players_stats(db_handler_func, matches=missing_player_stat_games)
    else:
        print("There are no matches to update!")


class Options(Enum):
    UPDATE_PLAYER_MATCH_STATS = CMDOption("Update player's stats for a particular match",
                                          update_players_stats, (db_handler_func,))
    UPDATE_DB_MATCH = CMDOption("Update score for a particular match in the database",
                                lambda x: "This will be implemented in the future")
    UPDATE_TEAM_MATCH_STATS = CMDOption("Update team's stats for a particular match", update_match_team_stats,
                                        (db_handler_func,))
    UPDATE_CHAMPIONSHIPS = CMDOption("Update EIHL championships in the DB", refresh_championships,
                                     (db_handler_func(),))
    UPDATE_MATCH_SCORES = CMDOption("Update DB with the latest EIHL matches", update_eihl_scores_from_game_centre,
                                    (db_handler_func(), True))
    REFRESH_DB = CMDOption("Update recent matches and stats", refresh_db)
    UPDATE_RECENT = CMDOption("Update recent matches and stats", update_recent_data)
    CHANGE_WEBSITE = CMDOption("Change Data Source", lambda x: "This will be implemented in the future")
    CHANGE_DATABASE = CMDOption("Change Database", lambda x: "This will be implemented in the future")
    HELP = CMDOption("You know what this function works you eejit!", display_help)
    EXIT = CMDOption("Exit the program", exit)


def main():
    """Main function that takes the user's input in the while loop
       and performs the function specified"""

    is_exit = False
    while not is_exit:
        user_input = input("What would you like to do? ->")
        if user_input is None:
            continue
        try:
            if Options[user_input].value.params:
                Options[user_input].value.action(*Options[user_input].value.params)
            else:
                Options[user_input].value.action()
        except KeyError:
            print("This is an invalid option!")


if __name__ == "__main__":
    main()
