from pyspark import SparkContext
import json
from itertools import combinations
from collections import defaultdict
import random
import sys

#python task1.py $ASNLIB/publicdata/train_review.json task1.res
sc = SparkContext("local[*]", "homework2_task1")
args = sys.argv

n = 50
b = 50
r = 1

def getBusinessUser(txt):
    json_dict = json.loads(txt)
    return ( json_dict['business_id'], json_dict['user_id'] )

def getBuss2idx(rdd):
    unq_buss = rdd.map(lambda x:x[0]).distinct().collect()
    return { b:i for b,i in zip( unq_buss, range(0, len(unq_buss)) ) }

def getUser2idx(rdd):
    unq_user = rdd.map(lambda x:x[1]).distinct().collect()
    return { u:i for u,i in zip( unq_user, range(0, len(unq_user)) ) }

def jaccardSimilarity(set1, set2): 
    return len(set1 & set2)/len(set1 | set2)

def hash_functions_generator(number_of_funcs, bins):
    p = int(1000000007)
    hash_funcs = []
    def build_func_typ1(a,b,m):
        def apply_func(x): return ((a*x)+b)%m
        return apply_func
    
    def build_func_typ2(a,b,m):
        def apply_func(x): return (((a*x)+b)%p)%m
        return apply_func
    
    for i in range(number_of_funcs):
        a = random.randint( 1, pow(2,20) )
        b = random.randint( 1, sys.maxsize//2 )
        if random.random() < 0.5: hash_funcs.append(build_func_typ1(a,b,bins))
        else: hash_funcs.append(build_func_typ2(a,b,bins))
    return hash_funcs

def get_minhash_signature(hash_funcs, values):
    signature = [ ]
    for func in hash_funcs:
        hash_v = float('inf')
        for v in values:
            hash_v = min(hash_v,  func(v) )
        signature.append(hash_v)
    return signature

def generate_band_hash(pair, b, r):
    buss_id = pair[0]
    signature = tuple(pair[1])
    for i in range(0,b,r):
        yield (  ( (signature[i:i+r], i//r), {buss_id} )  )

def buildOutput(x, business_user_map):
    sim = jaccardSimilarity( business_user_map[x[0]], business_user_map[x[1]] )
    return (x[0], x[1], sim)

rdd = sc.textFile(args[1].strip()) \
    .map( getBusinessUser ).distinct()

user2idx, buss2idx = getUser2idx(rdd), getBuss2idx(rdd)
total_users = len(user2idx)
idx2buss = { idx:b for b,idx in buss2idx.items() }

rdd_business = rdd.map( lambda x: ( buss2idx[x[0]], user2idx[x[1]] ) ) \
                    .groupByKey().mapValues(set)

hash_funcs = hash_functions_generator(n, int(total_users)-1)

candidates = rdd_business.mapValues( lambda user_ids: get_minhash_signature(hash_funcs, user_ids) ) \
    .flatMap( lambda x: generate_band_hash(x, b, r) ) \
    .reduceByKey( lambda x,y : x.union(y) ) \
    .filter(lambda x: len(x[1]) > 1) \
    .map(lambda x: x[1]) \
    .flatMap( lambda x: list(combinations(x, 2)) ) \
    .map( lambda x: (x[0], x[1]) if x[0] < x[1] else (x[1], x[0]) ) \
    .distinct()

business_users_map = rdd_business.collectAsMap()
result = candidates.map( lambda pair: buildOutput( pair, business_users_map ) ) \
    .filter( lambda x: x[2] >= 0.05 ) \
    .map( lambda x: (idx2buss[x[0]], idx2buss[x[1]], x[2]) if idx2buss[x[0]] < idx2buss[x[1]] else (idx2buss[x[1]], idx2buss[x[0]], x[2])) \
    .map(lambda x: "{"+f'"b1":"{x[0]}", "b2":"{x[1]}", "sim":{x[2]}'+"}") \
    .collect()

with open(args[2], "w") as out:
    out.write("\n".join(result))