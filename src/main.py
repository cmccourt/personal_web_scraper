import dataclasses
import traceback
from typing import Callable

import pandas as pd
from enum import Enum
from src.Exceptions import DBNotAvailable
from src.team_stats import update_all_players_stats, update_match_team_stats
from src.match import populate_all_eihl_matches


@dataclasses.dataclass(init=True)
class CMDOption:
    help: str or None
    action: Callable


def display_help():
    print("Help\n")
    for option in Options:
        print(f"{option.name}: {option.value.help}")


class Options(Enum):
    GET_MATCH_SCORE = CMDOption("Get score for match", lambda x: True)
    GET_TEAM_MATCH_STATS = CMDOption("Get team's stats for a particular match.", lambda x: True)
    GET_PLAYER_MATCH_STATS = CMDOption("Get player's stats for a particular match", lambda x: True)
    UPDATE_PLAYER_MATCH_STATS = CMDOption("Update player's stats for a particular match", update_all_players_stats)
    UPDATE_TEAM_MATCH_STATS = CMDOption("Update team's stats for a particular match", update_match_team_stats)
    UPDATE_MATCH_SCORES = CMDOption("Update DB with the latest EIHL matches", populate_all_eihl_matches)
    HELP = CMDOption("You know what this does you eejit!", display_help)
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
            Options[user_input].value.action()
        except KeyError:
            traceback.print_exc()
            print("This is a invalid option!")


if __name__ == "__main__":
    main()
