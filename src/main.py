import pandas as pd
from src.Exceptions import DBNotAvailable


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
            elif user_input == "-search":

                # user_input = input("Please enter your search: ")
                user_input = ""
                #team_stats: pd.DataFrame = get_team_stats(user_input)
                #print(team_stats)
            elif user_input == '-help':
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
