import networkx as nx


graph = nx.read_edgelist("/home/aditya/csci572/data/edges.txt")
page_rank = nx.pagerank(graph)
with open("external_pageRankFile.txt", "w") as f:
    for key, value in page_rank.items():
        f.write(f"/home/aditya/Downloads/solr-7.7.2/crawl_data/{key}={value}\n")