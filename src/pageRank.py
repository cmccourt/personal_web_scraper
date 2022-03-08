def computeRanks(graph):
    """Function that calculates the page ranking for each URL found"""
    d = 0.85
    num_loops = 10
    ranks = {}
    num_pages = len(graph)

    for page in graph:
        ranks[page] = 1.0 / num_pages

    for i in range(0, num_loops):
        new_ranks = {}
        for page in graph:
            new_rank = (1 - d) / num_pages
            for node in graph:
                if any(page in urls for urls in graph[node]):
                    new_rank = new_rank + d * (ranks[node] / len(graph[node]))
            new_ranks[page] = new_rank
        ranks = new_ranks
    return ranks
