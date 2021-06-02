import json
import sys
from operator import add

from pyspark import SparkContext

# python task3.py "datasets/review.json" "task3_default_ans.json" default 20 50
# python task3.py "datasets/review.json" "task3_customized_ans.json" customized 20 50
args = sys.argv

review_file = args[1].strip()
output_file = args[2].strip()
partition_type = args[3].strip()

result = dict()

sc = SparkContext.getOrCreate()

reviews_txt = sc.textFile(review_file)
reviews_json = reviews_txt.map( json.loads )

def custom_partitioner(key):
     return sum([ ord(c) for c in str(hash(key)) ])%int(args[4])

business_review = None
if partition_type == 'customized':
    business_review = reviews_json.map( lambda x: ( x['business_id'], 1 ) ) \
                            .partitionBy( int(args[4].strip()), custom_partitioner )
else:
    business_review = reviews_json.map( lambda x: ( x['business_id'], 1 ) )


result['n_partitions'] = business_review.getNumPartitions()
result['n_items'] = business_review.glom().map(len).collect()
result['result'] = business_review.reduceByKey(add) \
                        .filter( lambda x: x[1] > int(args[5].strip()) ) \
                        .collect()

with open(output_file, "w") as out:
    json.dump( result, out )


# spark-submit task3.py $ASNLIB/publicdata/review.json task3_default_ans default 20 50
# spark-submit task3.py $ASNLIB/publicdata/review.json task3_customized_ans customized 20 50

