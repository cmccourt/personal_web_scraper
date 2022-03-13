from collections import Counter, defaultdict

from src.Exceptions import DBNotAvailable

TOTAL_KEY = "--total"


def search_words_with_user_input(user_input, poodle_db):
    # User must have entered word(s) to search for
    try:
        search_result = search_input(user_input, poodle_db.index_graph.data, poodle_db.page_rank.data)
        if search_result:
            if TOTAL_KEY in search_result:
                # There has been a match with the multiple words given
                if len(search_result[TOTAL_KEY]) == 1:
                    # Only one matching URL, so we only need to print it
                    print(f"WOOF! {user_input} was found!")
                    print(f"{search_result[TOTAL_KEY]}")
                else:
                    # Need to organise the URLs based on their page ranking
                    order_urls = sorted(search_result[TOTAL_KEY], key=lambda x: x[1], reverse=True)
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


def search_input(user_input, index_graph: dict = None, page_rank=None):
    """Function to return list of urls found using input"""
    urls_found_rank = defaultdict(list)
    if not index_graph and not page_rank:
        return urls_found_rank
    # dictionary to extract the rank for the urls found

    # Capture all words in user input
    process_input = [x.strip() for x in user_input.split()]
    index_graph_search = {x.lower(): y for x, y in index_graph.items()}
    for word in process_input:
        # Check if the lowercase word matches any of the lowercase keys (improve chances of a match)
        if index_graph_search.get(word.lower(), None):
            # Append the url and rank to the dict
            urls_found_rank[word] = [[url, page_rank[url]] for url in index_graph_search[word.lower()]]
    # Get URLs that appear in more than 1 of the string inputs
    if len(process_input) > 1:
        # using a counter to keep track of the number of urls in the dict
        url_list = [value[0] for k, v in urls_found_rank.items() for value in v]
        counter = Counter(url_list)
        # Find the URLs that appear more than once
        matching_urls = [k for k, v in counter.items() if counter[k] > 1]
        if len(matching_urls) > 0:
            urls_found_rank[TOTAL_KEY] = [[url, page_rank[url]] for url in matching_urls]
    return urls_found_rank
