from bs4 import BeautifulSoup
import time
import requests
from random import randint
from html.parser import HTMLParser
import json

USER_AGENT = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

class SearchEngine:
    @staticmethod
    def search(query, sleep=True):
        if sleep:  # Prevents loading too many pages too soon
            time.sleep(randint(7, 20))
        temp_url = '+'.join(query.split())  # for adding + between words for the query
        url = 'http://www.bing.com/search?q=' + temp_url + "&count=30"
        soup = BeautifulSoup(requests.get(url, headers=USER_AGENT).text, "html.parser")
        new_results = SearchEngine.scrape_search_result(soup)
        return new_results

    @staticmethod
    def scrape_search_result(soup):
        raw_results = soup.find_all("li", attrs={"class": "b_algo"})
        results = []
        seen_links = set()
        # implement a check to get only 10 results and also check that URLs must not be duplicated
        for result in raw_results:
            link = result.find('a').get('href')
            if link not in seen_links:
                results.append(link)
                seen_links.add(link)
            if len(results) == 10:
                break
        return results


dictionary = {}
with open("hw1.json", "r") as fp:
    dictionary = json.load(fp)

i = 0
with open("queries_set1.txt", "r") as fp:
    for query in fp.readlines():
        query = query.strip()
        if len(dictionary.get(query, [])) > 5: continue
        dictionary[query] = SearchEngine.search(query)
        if len(dictionary[query]) <= 5:
            print(query, len(dictionary[query]))
            i += 1

print(f"Failed {i} times")

with open("hw1.json", "w") as fp:
    json.dump(dictionary, fp)
