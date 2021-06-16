from pyspark import SparkContext
import json
from itertools import combinations
from collections import defaultdict
import sys
import math
import random


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

def preprocess_user_based(txt):
    json_dict = json.loads(txt)
    return ( json_dict['user_id'], {json_dict['business_id']} )

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


def jaccardSimilarity(uid1, uid2, utility_matrix_map):
    intersection = len( utility_matrix_map[uid1].intersection(utility_matrix_map[uid2]) )
    union = len( utility_matrix_map[uid1].union(utility_matrix_map[uid2]) )
    return intersection/union

def have3CommonBusiness(uid1, uid2, utility_matrix_map):
    return len( utility_matrix_map[uid1].intersection(utility_matrix_map[uid2]) ) >= 3


def pearsonCorrelationUserBased(uid1, uid2, user_business_stars):
    commonBusiness = {x[0] for x in user_business_stars[uid1]}.intersection( {x[0] for x in user_business_stars[uid2]} )
    business_stars1 = { x[0]:x[1] for x in user_business_stars[uid1] if x[0] in commonBusiness }
    business_stars2 = { x[0]:x[1] for x in user_business_stars[uid2] if x[0] in commonBusiness }
    
    avg1 = sum(business_stars1.values())/len(business_stars1)
    avg2 = sum(business_stars2.values())/len(business_stars2)
    
    for k,v in business_stars1.items(): business_stars1[k] = v-avg1
    for k,v in business_stars2.items(): business_stars2[k] = v-avg2
    
    num, den1, den2 = 0, 0, 0
    for b in commonBusiness:
        num += business_stars1[b] * business_stars2[b]
        den1 += business_stars1[b]**2
        den2 += business_stars2[b]**2

    den = (math.sqrt(den1)*math.sqrt(den2))
    if den==0: return 0
    else: return num/den


def get_user_buss_stars(txt, user2idx, buss2idx):
    json_dict = json.loads(txt)
    return ( user2idx[json_dict['user_id']], 
               { (buss2idx[ json_dict['business_id']], json_dict['stars']) } )

def preprocess_item_based(txt):
    json_dict = json.loads(txt)
    return ( json_dict['business_id'], {json_dict['user_id']} )

def have3CommonUsers(bid1, bid2, utility_matrix_map):
    return len( utility_matrix_map[bid1].intersection(utility_matrix_map[bid2]) ) >= 3

def pearsonCorrelationItemBased(bid1, bid2, business_user_stars):
    commonUsers = {x[0] for x in business_user_stars[bid1]}.intersection( {x[0] for x in business_user_stars[bid2]} )
    user_stars1 = { x[0]:x[1] for x in business_user_stars[bid1] if x[0] in commonUsers }
    user_stars2 = { x[0]:x[1] for x in business_user_stars[bid2] if x[0] in commonUsers }
    
    avg1 = sum(user_stars1.values())/len(user_stars1)
    avg2 = sum(user_stars2.values())/len(user_stars2)
    
    for k,v in user_stars1.items(): user_stars1[k] = v-avg1
    for k,v in user_stars2.items(): user_stars2[k] = v-avg2
    
    num, den1, den2 = 0, 0, 0
    for b in commonUsers:
        num += user_stars1[b] * user_stars2[b]
        den1 += user_stars1[b]**2
        den2 += user_stars2[b]**2

    den = (math.sqrt(den1)*math.sqrt(den2))
    if den==0: return 0
    else: return num/den

def get_buss_user_stars(txt, user2idx, buss2idx):
    json_dict = json.loads(txt)
    return ( buss2idx[ json_dict['business_id']], 
               { (user2idx[json_dict['user_id']], json_dict['stars']) } )


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
        .map( lambda x: (idx2user[x[0]], idx2user[x[1]], pearsonCorrelationUserBased(x[0], x[1], user_business_stars)) ) \
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
        .map( lambda x: (idx2buss[x[0]], idx2buss[x[1]], pearsonCorrelationItemBased(x[0], x[1], business_user_stars)) ) \
        .filter(lambda x: x[2] > 0) \
        .map( lambda x: (x[0],x[1],x[2]) if x[0]<x[1] else (x[1],x[0],x[2]) ) \
        .distinct() \
        .map( lambda x: "{"+f'"b1":"{x[0]}", "b2":"{x[1]}", "sim":{x[2]}'+'}' ) \
        .collect()

    with open(model_file, "w") as out:
        out.write("\n".join(ans))