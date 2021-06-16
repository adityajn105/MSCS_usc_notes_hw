import json
import math
import random
import sys

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


def pearsonCorrelation(uid1, uid2, user_business_stars):
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
