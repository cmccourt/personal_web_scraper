from collections import defaultdict

import requests
from bs4 import BeautifulSoup


def get_page_hyperlinks(page):
    # Function that retrieves all URLs in the page
    links = []
    try:
        response = requests.get(page)
    except (requests.exceptions.RequestException, ValueError):
        return links
    bs4_soup = BeautifulSoup(response.content, 'html.parser')
    for url in bs4_soup.find_all("a", href=True):
        if url['href'] not in links:
            links.append(url['href'])
    return links


"""def get_all_new_links_on_page(page, level):
    #Function that retrieves all URLs in the page
    links, pos, all_found = [], 0, False
    try:
        response = requests.get(page[0])
        html = response.text
    except (requests.exceptions.RequestException, ValueError):
        return links

    while not all_found:
        a_tag = html.find("<a", pos)
        if a_tag > -1:
            href = html.find('href="', a_tag + 2)
            end_href = html.find('"', href + 6)
            url = html[href + 6:end_href]
            if url[:4] != 'mail':
                if url[:7] == "http://":
                    if url[-1] == "/":
                        url = url[:-1]
                if url[0:4] != 'http':
                    strip_url = page[0].strip()
                    if url in strip_url:
                        url = strip_url
                    else:
                        url = f"{strip_url}/{url}"
                if url not in [x[0] for x in links]:
                    links.append([url, level])
            close_tag = html.find("</a>", a_tag)
            pos = close_tag + 1
        else:
            all_found = True
    return links
"""


def get_url_links(seed_url):
    """Function that generates the URL graph"""

    to_crawl = [[seed_url, 0]]
    url_graph_data = defaultdict(list)

    # Used to check if url has been spotted
    url_spotted = []
    # TODO make the max_depth configurable in config file
    max_depth = 3
    while to_crawl:
        url_tup = to_crawl.pop()
        url: str = url_tup[0]
        level: int = url_tup[1]
        if url not in url_spotted:
            url_spotted.append(url)

        if level < max_depth:
            # new_links = get_page_hyperlinks(url, level + 1)
            new_links = get_page_hyperlinks(url)

            for new_link in new_links:
                url_graph_data[url].append(new_link)
                if new_link not in url_spotted:
                    url_spotted.append(new_link)
                    # link hasn't been crawled nor is it on the list to be crawled. Add the link to the to_crawl
                    to_crawl.append([new_link, level + 1])
    print("Crawling Complete!")
    if len(url_graph_data) == 1 and url_graph_data[seed_url] == []:
        # Couldn't load the URL given or there wasn't any links on the page
        return None
    else:
        return url_graph_data
