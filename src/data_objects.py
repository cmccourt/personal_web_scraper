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
        if file_path == "":
            file_path = self.default_file_path
        with open(file_path, "rb") as file:
            self.data = pickle.load(file)
            if self.data is not None:
                print("Loading Successful")

    def display_data(self):
        try:
            for url, value in self.data.items():
                print(f"{url} : {value}")
        except AttributeError:
            raise DBNotAvailable

    def __init__(self, _data=None, default_fp=None):
        self.data = _data
        self.default_file_path = default_fp


class URLGraph(BaseDBData):

    def __init__(self, _data=None, default_fp=None):
        if default_fp is None:
            default_fp = "../data/url_graph.txt"
        super().__init__(_data, default_fp)

    def build_url_graph(self):
        try:
            if self.data is None:
                self.data = webcrawler.get_url_links()
        except AttributeError:
            pass


class IndexGraph(BaseDBData):

    def __init__(self, _data=None, _url_graph_data=None, default_fp=None):
        if default_fp is None:
            default_fp = "../data/index_graph.txt"
        super().__init__(_data, default_fp)
        if _url_graph_data is not None:
            self.set_index_graph_data(_url_graph_data)

    def set_index_graph_data(self, url_graph):
        index = {}
        for key, value in url_graph.items():
            page_words = get_page_text(key)
            add_word_to_index(index, page_words, key)
        self.data = index


class PageRank(BaseDBData):

    def __init__(self, _data=None, _url_graph_data=None, default_fp=None):
        if default_fp is None:
            default_fp = "../data/page_rank.txt"
        super().__init__(_data, default_fp)
        if _url_graph_data is not None:
            self.set_page_ranks_data(_url_graph_data)

    def set_page_ranks_data(self, url_graph):
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
        self.data = ranks
