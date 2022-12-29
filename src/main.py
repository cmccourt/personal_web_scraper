import dataclasses
import traceback
from datetime import datetime
from enum import Enum
from typing import Callable

from src.data_handlers.eihl_postgres import EIHLPostgresHandler
from src.match import insert_all_eihl_matches
from src.player_stats import insert_all_players_stats
from src.team_stats import update_match_team_stats

# TODO make builder function to get data source handler
# TODO Create a MySQL handler
ds_handler = EIHLPostgresHandler()


@dataclasses.dataclass(init=True)
class CMDOption:
    help: str or None
    action: Callable
    params: None = None


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
            break
    return start_date, end_date


class Options(Enum):
    GET_MATCH_SCORE = CMDOption("Get score for match", lambda x: True)
    GET_TEAM_MATCH_STATS = CMDOption("Get team's stats for a particular match.", lambda x: True)
    GET_PLAYER_MATCH_STATS = CMDOption("Get player's stats for a particular match", lambda x: True)
    UPDATE_PLAYER_MATCH_STATS = CMDOption("Update player's stats for a particular match", insert_all_players_stats)
    UPDATE_TEAM_MATCH_STATS = CMDOption("Update team's stats for a particular match", update_match_team_stats)
    UPDATE_MATCH_SCORES = CMDOption("Update DB with the latest EIHL matches", insert_all_eihl_matches)
    CHANGE_DATA_SOURCE = CMDOption("Change Data Source", lambda x: "This will be implemented in the future")
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
