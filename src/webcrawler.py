from collections import defaultdict

import requests


def get_all_new_links_on_page(page, level):
    """Function that retrieves all of the URL in the page"""
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


def get_url_links():
    """Function that generates the URL graph"""
    seed_url = input("Please enter the URL: ")
    to_crawl = [[seed_url, 0]]
    url_graph_data = defaultdict(list)
    crawled = []
    # TODO make the max_depth configurable in config file
    max_depth = 1
    while to_crawl:
        url = to_crawl.pop()
        level = url[1]
        crawled.append(url)

        if level < max_depth:
            new_links = get_all_new_links_on_page(url, level + 1)
            if len(new_links) > 0:
                for link in new_links:
                    url_graph_data[url[0]].append(link)
                    if link[0] not in [x[0] for x in crawled] and link[0] not in [x[0] for x in to_crawl]:
                        # link hasn't been crawled nor is it on the list to be crawled. Add the link to the to_crawl
                        to_crawl.append(link)
    print("Crawling Complete!")
    if len(url_graph_data) == 1 and url_graph_data[seed_url] == []:
        # Couldn't load the URL given or there wasn't any links on the page
        return None
    else:
        return url_graph_data
