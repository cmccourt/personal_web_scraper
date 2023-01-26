import dataclasses
import traceback
from datetime import datetime
from enum import Enum
from typing import Callable

from src.data_handlers.eihl_mysql import EIHLMysqlHandler
# from src.data_handlers.eihl_postgres import EIHLPostgresHandler
from src.match import insert_all_eihl_matches_to_db
from src.player_stats import insert_all_players_stats
from src.team_stats import update_match_team_stats

# TODO make builder function to get data source handler
# ds_handler = EIHLPostgresHandler()
ds_handler = EIHLMysqlHandler()


@dataclasses.dataclass(init=True)
class CMDOption:
    help: str or None
    action: Callable
    params: object or None = None


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
            print("Invalid format! Format must be DD/MM/YY. Try again.")


def get_data_range() -> tuple[datetime, datetime]:
    while True:
        start_date = enter_date()
        end_date = enter_date()
        if start_date > end_date:
            print("Start date after end_date. Try again.")
        else:
            return start_date, end_date


# TODO Add option for updating championships
class Options(Enum):
    UPDATE_PLAYER_MATCH_STATS = CMDOption("Update player's stats for a particular match", insert_all_players_stats)
    UPDATE_TEAM_MATCH_STATS = CMDOption("Update team's stats for a particular match", update_match_team_stats)
    # UPDATE_CHAMPIONSHIPS = CMDOption("Update team's stats for a particular match", update_match_team_stats)
    UPDATE_MATCH_SCORES = CMDOption("Update DB with the latest EIHL matches", insert_all_eihl_matches_to_db, ds_handler)
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
            Options[user_input].value.action(Options[user_input].value.params)
        except KeyError:
            traceback.print_exc()
            print("This is an invalid option!")


if __name__ == "__main__":
    main()
