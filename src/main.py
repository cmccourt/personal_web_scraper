import searchWord
from src.Exceptions import DBNotAvailable
from src.PoodleDB import PoodleDB


def display_help():
    print("-build \t Create the POODLE database")
    print("-dump \t Save the POODLE database")
    print("-restore \t Retrieve the POODLE database")
    print("-print \t Show the POODLE database")
    print("-help \t Show this help information")
    print("-exit \t Exit the POODLE search engine")


def main():
    """Main function that takes the user's input in the while loop
       and performs the function specified"""

    poodle_db = PoodleDB()
    is_exit = False
    while not is_exit:
        try:
            user_input = input("WOOF! What would you like to do? ->")
            if user_input is None:
                continue
            if user_input == '-build':
                poodle_db.build_db()
            elif user_input == '-dump':
                # If user tries to save the graphs before building or restoring them, POODLE will prompt them
                try:
                    poodle_db.save_graphs()
                    print("Database saved")
                # The user has tried to search for words without the database.
                except AttributeError:
                    raise DBNotAvailable
            elif user_input == '-restore':
                index_fp = input("Enter file path for the index graph (Optional): ")
                page_rank_fp = input("Enter file path for the page rankings (Optional): ")
                url_graph_fp = input("Enter file path for the URL graph (Optional): ")
                poodle_db.restore_db_from_fp(index_fp, url_graph_fp, page_rank_fp)
            elif user_input == '-print':
                # If user tries to print the graphs before building or restoring them, POODLE will prompt them
                try:
                    poodle_db.display_graphs()
                    # The user has tried to search for words without the database.
                except AttributeError:
                    raise DBNotAvailable
            elif user_input == '-help':
                # Displays help list
                display_help()
            elif user_input == '-exit':
                # Exits the application
                is_exit = True
            # elif user_input[0] == '-':
            #     # The input given from user isn't valid
            #     print("WOOF! This is not a valid option. Use -help for list of functions")
            elif user_input == "-search":
                user_input = input("Please enter your search: ")
                searchWord.search_words_with_user_input(user_input, poodle_db)
            else:
                print("WOOF! This is not a valid option. Use -help for list of functions")
        except DBNotAvailable:
            print("WOOF! There is no database available. Please restore or build it")


if __name__ == "__main__":
    main()
