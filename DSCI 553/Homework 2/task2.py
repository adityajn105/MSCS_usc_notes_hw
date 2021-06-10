from pyspark import SparkContext
from itertools import combinations
from collections import defaultdict
from operator import add
import sys
from time import time

#python task2.py 70 50 "datasets/ub.csv" output2.txt
sc = SparkContext("local[*]", "homework2_task2")
args = sys.argv

start = time()
k = int(args[1].strip())

rdd = sc.textFile(args[3].strip()) \
        .filter(lambda x: x.split(',')[0] != 'user_id') \
        .map(lambda x: tuple(x.split(','))) \
        .map(lambda x: (x[0], frozenset({x[1]})) ) \
        .reduceByKey(lambda x, y : x|y ) \
        .filter(lambda x: len(x[1]) > k) \
        .map( lambda x: x[1] )

count = rdd.count()
support = int(args[2].strip())

def getOutputInFormat( itemset ):
    length_dict, mx = defaultdict(list), 0
    for item in itemset:
        length_dict[ len(item) ].append(tuple(sorted(item)))
        mx = max(mx, len(item))
    
    output = []
    for length in range(1, mx+1):
        vals = length_dict[length]
        if length == 1:
            output.append(",".join( sorted([f"('{v[0]}')" for v in vals]) ) )
        else:
            output.append(",".join( sorted([f"{v}" for v in vals]) ) )
    return "\n\n".join(output)

def getItemSet(partition, cnt, support):
    """Uses Apriori without any improvement"""
    baskets = []
    
    singletons = set()
    for basket in partition: 
        baskets.append( basket )
        singletons.update( basket )
    
    threshold = (support*len(baskets))/cnt
    
    #convert all candidates to tuple
    candidates = { frozenset( (c,) ) for c in singletons }
    frequent_candidates = set()
    cand_size = 1
    while len(candidates) > 0:
        #calculating count of each candidate from all baskets
        count = defaultdict(int)
        for basket in baskets:
            for candidate in candidates:
                if candidate.issubset(basket):
                    count[candidate] += 1

        #filtering candidates based on threshold
        frequent_itemset = set()
        for candidate in candidates:
            if count[candidate] >= threshold:
                frequent_itemset.add( candidate )
                frequent_candidates.add( candidate )
        
        #combining only frequeunt candidates to form new candidates of size+1
        candidates.clear()
        cand_size += 1
        for freq1, freq2 in combinations(frequent_itemset, 2):
            new_cand = freq1.union(freq2)
            valid = True
            if len(new_cand) != cand_size: continue
            for x in combinations(new_cand, cand_size-1):
                if frozenset(x) not in frequent_candidates:
                    valid = False
                    break
            if valid: candidates.add( new_cand )
    
    return frequent_candidates

def countFrequentItemset(partition, frequent_candidates):
    count = defaultdict(int)
    baskets = []
    for basket in partition:
        baskets.append(basket)
        
    for basket in baskets:
        for itemset in frequent_candidates:
            if itemset.issubset(basket):
                count[ itemset ]+=1
    
    freq_count = []
    for itemset, cnt in count.items():
        freq_count.append( (itemset, cnt) )
    return freq_count

frequent_candidates = rdd.mapPartitions( lambda x: getItemSet(x, count, support) ) \
                        .distinct().collect()
                        
frequent_itemsets = rdd.mapPartitions(lambda x: countFrequentItemset(x, frequent_candidates)) \
        .reduceByKey(add) \
        .filter( lambda x: x[1] >= support ) \
        .map(lambda x: x[0]) \
        .collect()

with open( args[4].strip(), "w" ) as out:
    print("Candidates:", file=out)
    print( getOutputInFormat(frequent_candidates), file=out )
    print(file=out)
    print("Frequent Itemsets:", file=out)
    print( getOutputInFormat(frequent_itemsets), file=out)

print(f"Duration: {time()-start}", file=sys.stdout)