import enum
import json

def modify_link(link):
    link = link.lstrip("https://")
    link = link.lstrip("http://")
    link = link.lstrip("www.")
    link = link.rstrip("/")
    return link

def remove_duplicates(links):
    seen = set()
    final = []
    for link in links:
        link = modify_link(link)
        if link not in seen:
            final.append(link)
        seen.add(link)
    return final

def spearman_coefficient(google, bing):
    overlap = set(google) & set(bing)
    google_idx = { link:i for i,link in  enumerate(google) if link in overlap }
    bing_idx = {link:i for i,link in enumerate(bing) if link in overlap }
    n = len(overlap)
    if n==0: return 0 
    if n == 1:
        link = list(overlap)[0]
        if google_idx[link] == bing_idx[link]:
            return 1
        else:
            return 0
    dsquare = 0
    for link in overlap:
        dsquare += (google_idx[link] - bing_idx[link])**2
    return 1 - ((6*dsquare)/(n*((n**2) - 1)))

def calculate_stats(google, bing):
    google = remove_duplicates(google)
    bing = remove_duplicates(bing)
    overlap = len(set(google) & set(bing))
    percent_overalap = overlap*100 / len(google)
    spearman = spearman_coefficient(google, bing)
    return overlap, percent_overalap, spearman


if __name__ == "__main__":
    with open("google_result_set1.txt", "r") as fp:
        google = json.load(fp)
    
    with open("hw1.json", "r") as fp:
        bing = json.load(fp)

    queries = google.keys()
    ans = []
    head = "Queries, Number of Overlapping Results, Percent Overlap, Spearman Coefficient"
    overlaps = percent_overalaps = spearmans = 0
    for i, query in enumerate(queries):
        overlap, percent_overalap, spearman = calculate_stats(google[query], bing[query])
        ans.append( f"Query {i+1}, {overlap}, {percent_overalap}, {spearman}" )
        overlaps += overlap
        percent_overalaps += percent_overalap
        spearmans += spearman
    avg = f"Average, {overlaps/100}, {percent_overalaps/100}, {spearmans/100}"

    with open("hw1.csv", "w") as fp:
        fp.write( head+"\n"+"\n".join(ans)+"\n"+avg )
    