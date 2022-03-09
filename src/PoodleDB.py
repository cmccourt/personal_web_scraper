import pickle

from src import webcrawler
from src.data_objects import URLGraph, PageRank, IndexGraph


class PoodleDB:

    def __init__(self, _url_graph: URLGraph, _index_graph: IndexGraph, _page_rank: PageRank):

        # If the seed URL given isn't valid then return to main menu
        if _url_graph is None:
            self.url_graph = URLGraph(webcrawler.get_url_links())
            if self.url_graph.data is None:
                print("URL could not be found.")
        else:
            self.url_graph = _url_graph
        if _index_graph is None:
            self.index_graph = IndexGraph(_url_graph=self.url_graph.data)
        else:
            self.index_graph = _index_graph
        if _page_rank is None:
            self.page_rank = PageRank(_url_graph=self.url_graph.data)
        else:
            self.page_rank = _page_rank
        print("POODLE Database created")

    def save_graphs(self):
        """Function that saves the url graph, index graph and
           page ranks into separate text files"""
        with open(self.index_graph.default_file_path, "w") as index_file:
            pickle.dump(self.index_graph.data, index_file)
        with open(self.url_graph.default_file_path, "w") as urls_file:
            pickle.dump(self.url_graph.data, urls_file)
        with open(self.page_rank.default_file_path, "w") as page_rank_file:
            pickle.dump(self.page_rank.data, page_rank_file)

    def display_graphs(self):
        """Function that displays the url graph, index graph and page rankings"""
        print("\n POODLE INDEX ----- \n ")
        self.index_graph.display_data()
        print("\nPOODLE GRAPH ----- \n ")
        self.url_graph.display_data()
        print("\nPOODLE RANKS ----- \n ")
        self.page_rank.display_data()
        print("\n")
