from pyspark import SparkContext
import json
from itertools import combinations
import sys

#python task1_groundtruth.py dataset//train_review.json train_review_ground_truth.res
sc = SparkContext("local[*]", "homework3_task1")
args = sys.argv

def getBusinessUser(txt):
    json_dict = json.loads(txt)
    return ( json_dict['business_id'], json_dict['user_id'] )

rdd = sc.textFile(args[1]).map( getBusinessUser )

def jaccardSimilarity(set1, set2): 
    return len(set1 & set2)/len(set1 | set2)


def getBuss2idx(rdd):
    unq_buss = rdd.map(lambda x:x[0]).distinct().collect()
    return { b:i for b,i in zip( unq_buss, range(0, len(unq_buss)) ) }

def getUser2idx(rdd):
    unq_user = rdd.map(lambda x:x[1]).distinct().collect()
    return { u:i for u,i in zip( unq_user, range(0, len(unq_user)) ) }

user2idx, buss2idx = getUser2idx(rdd), getBuss2idx(rdd)
idx2buss = { idx:b for b,idx in buss2idx.items() }

rdd_business = rdd.map( lambda x: ( buss2idx[x[0]], user2idx[x[1]] ) ) \
                    .groupByKey().mapValues(set)

business_usrs = {k: v for k,v in rdd_business.collect() }

ground_truth = []
for x,y in combinations( range(0, len(business_usrs)), 2 ):
    sim = jaccardSimilarity( business_usrs[x], business_usrs[y])
    if sim >= 0.05:
        if idx2buss[x] > idx2buss[y]:
            ground_truth.append( {"b1": idx2buss[y], "b2": idx2buss[x], "sim": sim} )
        else:
            ground_truth.append( {"b1": idx2buss[x], "b2": idx2buss[y], "sim": sim} )

with open(args[2], "w") as fp:
    fp.write("\n".join(map(lambda d: "{"+f'"b1":"{d["b1"]}", "b2":"{d["b2"]}", "sim":{d["sim"]}'+"}", ground_truth)))