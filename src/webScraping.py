import re

import requests
from bs4 import BeautifulSoup


def get_ignore_words():
    ignore_words = []
    try:
        with open("../settings/ignore_list.txt", "r") as ign_words_file:
            # Create a list of ignore words for comparison
            # convert word to lowercase and remove any whitespace
            ignore_words = [f"{word.lower().strip()} " for word in ign_words_file]
    except IOError:
        print("Could not find the file")
    return ignore_words


def get_page_text(url: str, ignore_words: list = None) -> str:
    response = requests.get(url)
    res_beaus = BeautifulSoup(response.content, 'html.parser')
    page_words: str = res_beaus.body.get_text()
    html_content = page_words.strip('\t\r\n')
    page_words = re.sub(r'\W', ' ', html_content)

    if ignore_words is None:
        ignore_words = get_ignore_words()
    ignore_reg = '|'.join(ignore_words)
    page_words = re.sub(ignore_reg, "", page_words)

    return page_words
