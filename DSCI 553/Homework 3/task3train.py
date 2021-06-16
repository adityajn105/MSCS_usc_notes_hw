from pyspark import SparkContext
from task3train_item import *
from task3train_user import *
import json
from itertools import combinations
from collections import defaultdict
import sys

#python task3train.py dataset/train_review.json task3user.model user_based
#python task3train.py dataset/train_review.json task3item.model item_based
sc = SparkContext("local[*]", "homework3_task3train")

def takeAvg(a):
  ans = defaultdict(list)
  for k,v in a: ans[k].append(v)
  return { (k, sum(v)/len(v)) for  k,v in ans.items() }

args = sys.argv

input_file = args[1]
model_file = args[2]
cf_type = args[3]

if cf_type=='user_based':
    user_business = sc.textFile(input_file).map(preprocess_user_based) \
        .reduceByKey(lambda x,y: x|y)

    unq_users = user_business.map(lambda x: x[0]).distinct().collect()
    user2idx = { usr:idx for usr,idx in zip( unq_users, range(0,len(unq_users)) ) }
    idx2user = { idx:usr for usr,idx in user2idx.items() }

    unq_business = sc.textFile(input_file) \
        .map(lambda x: json.loads(x)['business_id']) \
        .distinct().collect()

    buss2idx = {buss:idx for buss,idx in zip(unq_business, range(0, len(unq_business)))}

    n = 50
    b = 50
    r = 1
    hash_funcs = hash_functions_generator(n, len(buss2idx)-1)

    candidates = user_business.map( lambda x: (user2idx[x[0]], {buss2idx[z] for z in x[1]}) ) \
        .mapValues( lambda buss_ids: get_minhash_signature(hash_funcs, buss_ids) ) \
        .flatMap( lambda x: generate_band_hash(x, b, r) ) \
        .reduceByKey( lambda x,y : x.union(y) ) \
        .filter(lambda x: len(x[1]) > 1) \
        .map(lambda x: x[1]) \
        .flatMap( lambda x: list(combinations(x, 2)) ) \
        .map( lambda x: (x[0], x[1]) if x[0] < x[1] else (x[1], x[0]) ) \
        .groupByKey() \
        .mapValues(set) \
        .flatMap( lambda x: [ (x[0], y) for y in x[1] ] )
    
    utility_matrix_map = user_business.map(lambda x: (user2idx[x[0]], {buss2idx[z] for z in x[1]})) \
            .collectAsMap()

    user_business_stars = sc.textFile(input_file) \
        .map(lambda x: get_user_buss_stars(x, user2idx, buss2idx)) \
        .reduceByKey( lambda x,y: x|y ) \
        .mapValues(takeAvg) \
        .collectAsMap()

    ans = candidates.map( lambda x: (x[0], x[1], jaccardSimilarity(x[0], x[1], utility_matrix_map)) ) \
        .filter( lambda x: x[2] >= 0.01 and have3CommonBusiness(x[0], x[1], utility_matrix_map) ) \
        .map( lambda x: (idx2user[x[0]], idx2user[x[1]], pearsonCorrelation(x[0], x[1], user_business_stars)) ) \
        .filter(lambda x: x[2] > 0) \
        .map( lambda x: (x[0],x[1],x[2]) if x[0]<x[1] else (x[1],x[0],x[2]) ) \
        .distinct() \
        .map( lambda x: "{"+f'"u1":"{x[0]}", "u2":"{x[1]}", "sim":{x[2]}'+'}' ) \
        .collect()

    with open(model_file, "w") as out:
        out.write("\n".join(ans))

else:
    business_user = sc.textFile(input_file).map(preprocess_item_based) \
            .reduceByKey(lambda x,y: x|y)

    unq_business = business_user.map(lambda x: x[0]).distinct().collect()
    buss2idx = { buss:idx for buss,idx in zip( unq_business, range(0,len(unq_business)) ) }
    idx2buss = { idx:buss for buss,idx in buss2idx.items() }

    unq_user = sc.textFile(input_file).map(lambda x: json.loads(x)['user_id']) \
        .distinct().collect()
    user2idx = {user:idx for user,idx in zip(unq_user, range(0, len(unq_user)))}
    
    utility_matrix_map = business_user.map(lambda x: (buss2idx[x[0]], {user2idx[z] for z in x[1]})).collectAsMap()

    business_user_stars = sc.textFile(input_file) \
        .map(lambda x: get_buss_user_stars(x, user2idx, buss2idx)) \
        .reduceByKey( lambda x,y: x|y ) \
        .mapValues(takeAvg) \
        .collectAsMap()

    ans = sc.parallelize( list( range(len(buss2idx)) ) ) \
        .flatMap(  lambda x: [ (i,x) for i in range(x) ]  ) \
        .filter( lambda x: have3CommonUsers(x[0], x[1], utility_matrix_map) ) \
        .map( lambda x: (idx2buss[x[0]], idx2buss[x[1]], pearsonCorrelation(x[0], x[1], business_user_stars)) ) \
        .filter(lambda x: x[2] > 0) \
        .map( lambda x: (x[0],x[1],x[2]) if x[0]<x[1] else (x[1],x[0],x[2]) ) \
        .distinct() \
        .map( lambda x: "{"+f'"b1":"{x[0]}", "b2":"{x[1]}", "sim":{x[2]}'+'}' ) \
        .collect()

    with open(model_file, "w") as out:
        out.write("\n".join(ans))