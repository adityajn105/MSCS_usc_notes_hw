import json
import math

def preprocess_item_based(txt):
    json_dict = json.loads(txt)
    return ( json_dict['business_id'], {json_dict['user_id']} )

def have3CommonUsers(bid1, bid2, utility_matrix_map):
    return len( utility_matrix_map[bid1].intersection(utility_matrix_map[bid2]) ) >= 3

def pearsonCorrelation(bid1, bid2, business_user_stars):
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