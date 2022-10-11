import pickle

from src.data_objects import URLGraph, PageRank, IndexGraph


class EIHLDbGraphs:

    def __init__(self, _url_graph: URLGraph = None, _index_graph: IndexGraph = None, _page_rank: PageRank = None):
        self.url_graph = URLGraph()
        self.index_graph = IndexGraph()
        self.page_rank = PageRank()
        if _url_graph is not None and _index_graph is not None and _page_rank is not None:
            self.build_db(_url_graph, _index_graph, _page_rank)

    def build_db(self, url_graph=None, index_graph=None, page_rank=None):
        # If the seed URL given isn't valid then return to main menu
        if url_graph is None:
            self.url_graph.build_url_graph()
            if self.url_graph.data is None:
                print("URL could not be found.")
        else:
            self.url_graph = url_graph

        if index_graph is None:
            self.index_graph.set_index_graph_data(self.url_graph.data)
        else:
            self.index_graph = index_graph

        if page_rank is None:
            self.page_rank.set_page_ranks_data(self.url_graph.data)
        else:
            self.page_rank = page_rank
        print("POODLE Database created")

    def restore_db_from_fp(self, index_fp, url_fp, page_rank_fp):
        self.index_graph.load_data_from_file(index_fp)
        self.page_rank.load_data_from_file(page_rank_fp)
        self.url_graph.load_data_from_file(url_fp)

    def save_graphs(self):
        """Function that saves the url graph, index graph and
           page ranks into separate text files"""
        with open(self.index_graph.default_file_path, "wb") as index_file:
            pickle.dump(self.index_graph.data, index_file)
        with open(self.url_graph.default_file_path, "wb") as urls_file:
            pickle.dump(self.url_graph.data, urls_file)
        with open(self.page_rank.default_file_path, "wb") as page_rank_file:
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
