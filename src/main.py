import searchWord
from data_objects import URLGraph, IndexGraph, PageRank
from src.Exceptions import DBNotAvailable
from src.PoodleDB import PoodleDB


def display_help():
    print("-build \t Create the POODLE database")
    print("-dump \t Save the POODLE database")
    print("-restore \t Retrieve the POODLE database")
    print("-print \t Show the POODLE database")
    print("-help \t Show this help information")
    print("-exit \t Exit the POODLE search engine")


def search_words_with_user_input(user_input, poodle_db):
    # User must have enter word(s) to search for
    try:
        search_result = searchWord.search_input(user_input, poodle_db.index_graph, poodle_db.page_rank)
        if search_result:
            if "Overall" in search_result:
                # There has been a match with the multiple words given
                if len(search_result["Overall"]) == 1:
                    # Only one matching URL so we only need to print it
                    print(f"WOOF! {user_input} was found!")
                    print(f"{search_result['Overall']}")
                else:
                    # Need to organise the URLs based on their page ranking
                    order_urls = sorted(search_result["Overall"], key=lambda x: x[1], reverse=True)
                    print(f"WOOF! {user_input} was found!")
                    for url_rank in order_urls:
                        print(f"{url_rank[0]} : {url_rank[1]}")
            else:
                # Couldn't find a common URL for the word(s) given so display the individual words instead
                print(f"WOOF! {user_input} could not be found but here the individual words were found!")
                url_tuple = sorted(search_result.items(), reverse=True, key=lambda x: x[1])
                for url_rank in url_tuple:
                    print(f"{url_rank[0]}: ")
                    for url in url_rank[1]:
                        print(f"{url}")
        else:
            # User input couldn't be found
            print(f"WOOF! {user_input} could not be found")
    # The user has tried to search for words without the database.
    except AttributeError:
        raise DBNotAvailable


def main():
    """Main function that takes the user's input in the while loop
       and performs the function specified"""

    poodle_db = None
    is_exit = False
    while not is_exit:
        try:
            user_input = input("WOOF! What would you like to do? ->")
            if user_input is None:
                continue
            if user_input == '-build':
                url_graph = URLGraph()
                index_graph = IndexGraph()
                page_rank = PageRank()
                poodle_db = PoodleDB(url_graph, index_graph, page_rank)
            elif user_input == '-dump':
                # If user tries to save the graphs before building or restoring them, POODLE will prompt them
                try:
                    poodle_db.save_graphs()
                    print("Database saved")
                # The user has tried to search for words without the database.
                except AttributeError:
                    raise DBNotAvailable
            elif user_input == '-restore':
                index_graph = IndexGraph(input("Enter file path for the index graph"))
                page_rank = PageRank(input("Enter file paths for the page rankings"))
                url_graph = URLGraph(input("Enter file paths for the URL graph"))
                poodle_db = PoodleDB(url_graph, index_graph, page_rank)
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
            elif user_input[0] == '-':
                # The input given from user isn't valid
                print("WOOF! This is not a valid option. Use -help for list of functions")
            else:
                search_words_with_user_input(user_input, poodle_db)
        except DBNotAvailable:
            print("WOOF! There is no database available. Please restore or build it")


if __name__ == "__main__":
    main()
