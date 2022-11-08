import pandas as pd
from enum import Enum
from src.Exceptions import DBNotAvailable
from src.team_stats import get_all_players_stats, get_match_team_stats
from src.match import populate_all_eihl_matches


class Options(Enum):
    GET_MATCHES = "--get_match_scores"
    GET_TEAM_MATCH_STATS = "--get_team_match_stats"
    GET_PLAYER_MATCH_STATS = "--get_player_match_stats"


def display_help():
    print("-build \t Create the EIHL database")
    print("-dump \t Save the EIHL database")
    print("-restore \t Retrieve the EIHL database")
    print("-print \t Show the EIHL database")
    print("-help \t Show this help information")
    print("-exit \t Exit the EIHL search engine")


def main():
    """Main function that takes the user's input in the while loop
       and performs the function specified"""

    is_exit = False
    while not is_exit:
        try:
            user_input = input("What would you like to do? ->")
            if user_input is None:
                continue
            elif user_input == "-update_match_scores":
                populate_all_eihl_matches()
            elif user_input == '-update_player_match_stats':
                get_all_players_stats()
            elif user_input == '-update_team_match_stats':
                get_match_team_stats()
            elif user_input == "-help":
                # Displays help list
                display_help()
            elif user_input == '-exit':
                # Exits the application
                is_exit = True
            else:
                print("ERROR! This is not a valid option. Use -help for list of functions")
        except DBNotAvailable:
            print("ERROR! There is no database available. Please restore or build it")


if __name__ == "__main__":
    main()
