from pyspark import SparkContext
import json
import sys
sc = SparkContext.getOrCreate()


#python task3_grader.py test_review_rating.json task3.res item_based
ground_truth = sc.textFile(sys.argv[1]).map(json.loads) \
    .map( lambda x: ( (x['business_id'], x['user_id']), x['stars']) ) \
    .collectAsMap()

our_truth = sc.textFile(sys.argv[2]).map(json.loads) \
  .map( lambda x: ( (x['business_id'], x['user_id']), x['stars']) ) \
  .collectAsMap()


common = set(ground_truth.keys()) & set(our_truth.keys())

assert len(common) == len(ground_truth), f"Items different in Ground Truth {len(ground_truth)} and Input {len(our_truth)}"

sm = 0
for c in common:
  sm +=  ( ground_truth[c]-our_truth[c] )**2
sm = sm/len(common)

sm = sm**0.5

print('RMSE', sm)

if sys.argv[3] == 'item_based':
  assert sm <= 0.91, f"Failed, RMSE > 0.91"
else:
  assert sm <= 1.01, f"Failed, RMSE >= 1.01"