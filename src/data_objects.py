import pickle

from src import webcrawler
from src.Exceptions import DBNotAvailable
from src.webScraping import get_page_text, add_word_to_index


class BaseDBData:

    def load_data_from_file(self, file_path):
        """
        Function that loads the graph's text file
        Args:
            file_path (str): File path of graph

        Returns:
            dict: Parsed Graph

        """
        with open(file_path, "r") as file:
            self.data = pickle.load(file)

    def display_data(self):
        try:
            for url, value in self.data.items():
                print(f"{url} : {value}")
        except AttributeError:
            raise DBNotAvailable

    def __init__(self, _data=None):
        self.data = _data


class URLGraph(BaseDBData):

    def __init__(self, _data=None):
        self.default_file_path = "../data/url_graph.txt"
        super().__init__(_data)
        try:
            if _data is None:
                self.data = webcrawler.get_url_links()
        except AttributeError:
            pass


class IndexGraph(BaseDBData):

    def __init__(self, _data=None, _url_graph=None):
        self.default_file_path = "../data/index_graph.txt"
        if _url_graph is not None:
            self.data = self.get_index_graph(_url_graph)
        else:
            super().__init__(_data)

    @staticmethod
    def get_index_graph(url_graph):
        index = {}
        for key, value in url_graph.items():
            page_words = get_page_text(key)
            add_word_to_index(index, page_words, key)
        return index


class PageRank(BaseDBData):

    def __init__(self, _data=None, _url_graph=None):
        self.default_file_path = "../data/page_rank.txt"
        if _url_graph is not None:
            self.data = self.compute_ranks(_url_graph)
        else:
            super().__init__(_data)

    @staticmethod
    def compute_ranks(url_graph):
        """Function that calculates the page ranking for each URL found"""
        d = 0.85
        num_loops = 10
        ranks = {}
        num_pages = len(url_graph)

        for page in url_graph:
            ranks[page] = 1.0 / num_pages

        for i in range(0, num_loops):
            new_ranks = {}
            for page in url_graph:
                new_rank = (1 - d) / num_pages
                for node in url_graph:
                    if any(page in urls for urls in url_graph[node]):
                        new_rank = new_rank + d * (ranks[node] / len(url_graph[node]))
                new_ranks[page] = new_rank
            ranks = new_ranks
        return ranks
