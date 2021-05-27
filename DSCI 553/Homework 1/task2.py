from pyspark import SparkContext
import json
import sys
from collections import defaultdict

result = dict()
args = sys.argv

review_file = args[1]
business_file = args[2]
output_file = args[3]
if_spark = args[4]=='spark'

if if_spark:
    # python task2.py "datasets/review.json" "datasets/business.json" "output.json" spark 10 
    sc = SparkContext.getOrCreate()

    reviews_txt = sc.textFile(review_file)
    reviews_json = reviews_txt.map( json.loads )

    business_txt = sc.textFile(business_file)
    business_json = business_txt.map( json.loads )

    review_json1 = reviews_json.map( lambda x: (x['business_id'], x['stars']) )
    def business_process(js):
        if js['categories'] is None: return []
        return [ (js['business_id'], cat.strip()) for cat in js.get('categories', "").split(',') ]

    business_json1 = business_json.flatMap(business_process)

    result = review_json1.join(business_json1) \
            .map(lambda x: (x[1][1], (x[1][0], 1))) \
            .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
            .map( lambda x: (x[0], x[1][0]/x[1][1]) ) \
            .sortBy( lambda x: ( -1*x[1], x[0] ) ) \
            .take( int(args[5]) )

    with open(output_file, "w") as out:
        json.dump({'result':result}, out)

else:
    # python task2.py "datasets/review.json" "datasets/business.json" "output.json" no_spark 10 
    reviews_stars  = defaultdict(list)
    with open(review_file) as fp:
        for js in fp.readlines():
            js = json.loads(js)
            reviews_stars[ js['business_id'] ].append( js['stars'] )

    business_cat = defaultdict(list)
    with open(business_file) as fp:
        for js in fp.readlines():
            js = json.loads(js)
            if js['categories'] is None: continue
            business_cat[ js['business_id'] ].extend( [ cat.strip() for cat in js.get('categories', "").split(',')] )

    business_cat_stars = []
    for b_id, ratings in reviews_stars.items():
        for cat in business_cat[b_id]:
            for rating in ratings:
                business_cat_stars.append( (cat, rating) )
    
    category = defaultdict(list)
    for cat, rating in business_cat_stars:
        category[cat].append(rating)

    result = []
    for key,val in category.items():
        result.append( (key, sum(val)/len(val)) )
    result.sort( key=lambda x: ( -1*x[1], x[0] ) )

    with open(output_file, "w") as out:
        json.dump({'result':[ list(ans) for ans in result[ :int(args[5]) ] ]}, out)


# spark-submit task2.py $ASNLIB/publicdata/review.json $ASNLIB/publicdata/business.json task2_no_spark_ans no_spark 20
# spark-submit task2.py $ASNLIB/publicdata/review.json $ASNLIB/publicdata/business.json task2_spark_ans spark 20