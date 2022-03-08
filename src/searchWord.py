from collections import Counter


def search_input(user_input, word_graph=None, page_rank=None):
    """Function to return list of urls found using input"""
    if not word_graph and not page_rank:
        return None
    # dictionary to extract the rank for the urls found
    urls_found_rank = {}
    process_input = []
    if len(user_input.split()) > 1:
        # more than word present.
        for word in user_input.split():
            process_input.append(word)
    else:
        # Just one word given. Remove whitespace
        process_input.append(user_input.replace(" ", ""))
    for word in process_input:
        for key, value in word_graph.items():
            # Check if the lowercase word matches the lowercase key (improve chances of a match)
            if word.lower() == key.lower():
                for url in value:
                    if word in urls_found_rank.keys():
                        # Append the url and rank to the dict
                        urls_found_rank[word].append([url, page_rank[url]])
                    else:
                        urls_found_rank[word] = [[url, page_rank[url]]]
    if len(process_input) > 1:
        # using a counter to keep track of the url values in the dict
        url_list = [value[0] for k, v in urls_found_rank.items() for value in v]
        counter = Counter(url_list)
        # Find the matching URLs using the counter
        matching_urls = [k for k, v in counter.items() if counter[k] > 1]
        if len(matching_urls) > 0:
            for match_url in matching_urls:
                if "Overall" in urls_found_rank.keys():
                    urls_found_rank["Overall"].append([match_url, page_rank[url]])
                else:
                    urls_found_rank["Overall"] = [[match_url, page_rank[match_url]]]
    return urls_found_rank
