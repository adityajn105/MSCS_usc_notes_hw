import json
from pyspark import SparkContext
import sys
import math

sc = SparkContext("local[*]", "homework3_task2predict")

#python task2predict.py dataset/test_review.json task2model.model task2.predict 
args = sys.argv

input_file = args[1]
model_path = args[2]
output_path = args[3]

model = sc.textFile(model_path).map(json.loads) \
    .map( lambda x: (x['type'], x['id'], x['value']) )


userProfiles = model.filter( lambda x: x[0] == 'User' ) \
    .map( lambda x: (x[1], x[2]) ) \
    .collectAsMap()

docProfiles = model.filter( lambda x: x[0] == 'Document' ) \
    .map( lambda x: (x[1], x[2]) ) \
    .collectAsMap()

def cosine_similarity(bid, uid, userProfiles, docProfiles):
    if uid not in userProfiles or bid not in docProfiles: return 0
    uv, bv = set(userProfiles[uid]), set(docProfiles[bid])
    return len(uv.intersection(bv)) / (math.sqrt(len(uv)) * math.sqrt(len(bv)))

predicted = sc.textFile(input_file).map( json.loads ) \
    .map( lambda x: (x['business_id'], x['user_id']) ) \
    .map( lambda x: (x[0], x[1], cosine_similarity(x[0], x[1], userProfiles, docProfiles) ) ) \
    .filter(lambda x: x[2] >= 0.01) \
    .map( lambda x: "{"+f'"user_id":"{x[1]}", "business_id":"{x[0]}", "sim":{x[2]}'+"}" ) \
    .collect()

with open(output_path, "w") as out:
    out.write("\n".join(predicted))