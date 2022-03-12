from collections import Counter, defaultdict

from src.Exceptions import DBNotAvailable


def search_words_with_user_input(user_input, poodle_db):
    # User must have entered word(s) to search for
    try:
        search_result = search_input(user_input, poodle_db.index_graph.data, poodle_db.page_rank.data)
        if search_result:
            if "Overall" in search_result:
                # There has been a match with the multiple words given
                if len(search_result["Overall"]) == 1:
                    # Only one matching URL, so we only need to print it
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


def search_input(user_input, word_graph=None, page_rank=None):
    """Function to return list of urls found using input"""
    if not word_graph and not page_rank:
        return None
    # dictionary to extract the rank for the urls found
    urls_found_rank = defaultdict(list)
    # Capture all words in user input
    process_input = [x.strip() for x in user_input.split()]
    for word in process_input:
        for key, value in word_graph.items():
            # Check if the lowercase word matches the lowercase key (improve chances of a match)
            if word.lower() == key.lower():
                for url in value:
                    # Append the url and rank to the dict
                    urls_found_rank[word].append([url, page_rank[url]])
    if len(process_input) > 1:
        # using a counter to keep track of the url values in the dict
        url_list = [value[0] for k, v in urls_found_rank.items() for value in v]
        counter = Counter(url_list)
        # Find the matching URLs using the counter
        matching_urls = [k for k, v in counter.items() if counter[k] > 1]
        if len(matching_urls) > 0:
            for match_url in matching_urls:
                urls_found_rank["Overall"].append([match_url, page_rank[match_url]])
    return urls_found_rank
