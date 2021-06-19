from pyspark import SparkContext
import sys
import json
from collections import defaultdict

#spark-submit task3predict.py $ASNLIB/publicdata/train_review.json $ASNLIB/publicdata/test_review.json task3item.model task3item.predict item_based
#spark-submit task3predict.py $ASNLIB/publicdata/train_review.json $ASNLIB/publicdata/test_review.json task3user.model task3user.predict user_based
sc=SparkContext("local[*]", "homework3_task3predict")

args = sys.argv

train_input_file = args[1]
test_input_file = args[2]
model_file = args[3]
output_file = args[4]
cf_type = args[5]

train_input = sc.textFile(train_input_file).map(json.loads)\
        .map(lambda x: (x['business_id'], x['user_id'], x['stars']) )

unq_users = train_input.map(lambda x: x[1]).distinct().collect()
unq_users.append('UNK')
user2idx = { usr:idx for usr,idx in zip( unq_users, range(len(unq_users))) }
idx2user = { idx:usr for usr,idx in user2idx.items()}

unq_business = train_input.map(lambda x: x[0]).distinct().collect()
unq_business.append('UNK')
buss2idx = { buss:idx for buss,idx in zip(unq_business, range(len(unq_business))) }
idx2buss = { idx:buss for buss,idx in buss2idx.items()}

UNK_USER_ID = user2idx['UNK']
UNK_BUSS_ID = buss2idx['UNK']

def predict(x, N, avg_dict, model, unk_id, cf_type):

    if cf_type=='item_based':
        target_id = x[0]
        target_id_avg = avg_dict.get(target_id, avg_dict[unk_id])

        result = []
        for id, score in x[1]:
            if target_id < id: key = (target_id, id)
            else: key = (id, target_id)
            result.append( (score, model.get(key,0)) )

        score_similarity = sorted(result, key=lambda item: item[1], reverse=True)[:N]
        
        num = sum( [item1*item2 for item1, item2 in score_similarity] )
        if num==0: return (target_id, target_id_avg)

        den = sum( [item[1] for item in score_similarity] )
        if den==0: return (target_id, target_id_avg)

        return (target_id, (num/den))
    else:
        target_id = x[0]
        target_id_avg = avg_dict.get(target_id, avg_dict[unk_id])

        result = []
        for id, score in x[1]:
            if target_id < id: key = (target_id, id)
            else: key = (id, target_id)
            result.append( (score-avg_dict.get(id, avg_dict[unk_id]), model.get(key,0)) )
        
        score_similarity = sorted(result, key=lambda item: item[1], reverse=True)[:N]
        
        num = sum( [item1*item2 for item1, item2 in score_similarity] )
        if num==0: return (target_id, target_id_avg)
        
        den = sum( [item[1] for item in score_similarity] )
        if den==0: return (target_id, target_id_avg)
        
        return (target_id, target_id_avg+(num/den))

def takeAvg(a):
  ans = defaultdict(list)
  for k,v in a: ans[k].append(v)
  return { (k, sum(v)/len(v)) for  k,v in ans.items() }

if cf_type == 'item_based':
    model = sc.textFile(model_file) \
        .map(json.loads) \
        .map(lambda x: (buss2idx.get(x['b1'], -1), buss2idx.get(x['b2'], -1), x['sim'])) \
        .filter( lambda x: x[0]!=-1 or x[1]!=-1 ) \
        .map(lambda x: ( (x[0], x[1]), x[2]) if x[0]<x[1] else ((x[1], x[0]), x[2])) \
        .collectAsMap()

    train = train_input.map(lambda x: (user2idx[x[1]], (buss2idx[x[0]], x[2]))) \
        .groupByKey() \
        .mapValues(takeAvg)

    buss_avg_dict = train_input.map( lambda x: (buss2idx[x[0]], (x[2],1)) ) \
            .reduceByKey(lambda x,y: ((x[0]+y[0]),(x[1]+y[1]))) \
            .mapValues(lambda x: x[0]/x[1]).collectAsMap()
    buss_avg_dict[UNK_BUSS_ID] = sum(buss_avg_dict.values())/len(buss_avg_dict)

    test = sc.textFile(test_input_file).map(json.loads).map(lambda x: (x['business_id'], x['user_id']))

    not_available = test.filter( lambda x: (x[0] not in buss2idx) or (x[1] not in user2idx) ) \
            .map(lambda x: "{"+f'"user_id":"{x[1]}", "business_id":"{x[0]}", "stars":{buss_avg_dict[UNK_BUSS_ID]}'+"}") \
            .collect()

    test = test.filter( lambda x: (x[0] in buss2idx) and (x[1] in user2idx) ) \
            .map(lambda x: (user2idx[x[1]], buss2idx[x[0]]))

    output = test.leftOuterJoin(train) \
        .mapValues( lambda x: predict(x, 5, buss_avg_dict, model, UNK_BUSS_ID, cf_type) ) \
        .map( lambda x: ( idx2user[x[0]], idx2buss[x[1][0]], x[1][1]) ) \
        .map(lambda x: "{"+f'"user_id":"{x[0]}", "business_id":"{x[1]}", "stars":{x[2]}'+"}") \
        .collect()

    with open(output_file, "w") as fp:
        fp.write( "\n".join(output) )
        fp.write("\n")
        fp.write( "\n".join(not_available) )

else:
    model = sc.textFile(model_file) \
        .map(json.loads) \
        .map(lambda x: (user2idx.get(x['u1'], -1), user2idx.get(x['u2'], -1), x['sim'])) \
        .filter( lambda x: x[0]!=-1 or x[1]!=-1 ) \
        .map(lambda x: ( (x[0], x[1]), x[2]) if x[0]<x[1] else ((x[1], x[0]), x[2])) \
        .collectAsMap()

    train = train_input.map(lambda x: (buss2idx[x[0]], (user2idx[x[1]], x[2]))) \
        .groupByKey() \
        .mapValues(takeAvg)

    user_avg_dict = train_input.map( lambda x: (user2idx[x[1]], (x[2],1)) ) \
            .reduceByKey(lambda x,y: ((x[0]+y[0]),(x[1]+y[1]))) \
            .mapValues(lambda x: x[0]/x[1]).collectAsMap()
    user_avg_dict[UNK_USER_ID] = sum(user_avg_dict.values())/len(user_avg_dict)

    test = sc.textFile(test_input_file).map(json.loads).map(lambda x: (x['business_id'], x['user_id']))
    
    not_available = test.filter( lambda x: (x[0] not in buss2idx) or (x[1] not in user2idx) ) \
            .map(lambda x: "{"+f'"user_id":"{x[1]}", "business_id":"{x[0]}", "stars":{user_avg_dict[UNK_USER_ID]}'+"}") \
            .collect()

    test = test.filter( lambda x: (x[0] in buss2idx) and (x[1] in user2idx) ) \
            .map(lambda x: (buss2idx.get(x[0]),  user2idx.get(x[1])))

    output = test.leftOuterJoin(train) \
        .mapValues( lambda x: predict(x, 5, user_avg_dict, model, UNK_USER_ID, cf_type) ) \
        .map( lambda x: ( idx2buss[x[0]], idx2user[x[1][0]], x[1][1]) ) \
        .map(lambda x: "{"+f'"user_id":"{x[1]}", "business_id":"{x[0]}", "stars":{x[2]}'+"}") \
        .collect()

    with open(output_file, "w") as fp:
        fp.write( "\n".join(output) )
        fp.write("\n")
        fp.write( "\n".join(not_available) )