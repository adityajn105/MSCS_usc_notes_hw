from pyspark import SparkContext
import json

sc = SparkContext.getOrCreate()

def preprocess_business(txt):
    json_dict = json.loads(txt)
    return (json_dict['business_id'], json_dict['state'])
    
business = sc.textFile('datasets/business.json') \
            .map(preprocess_business) \
            .filter(lambda x: x[1]=='NV') \
            .map(lambda x: (x[0],1))

def preprocess_reviews(txt):
    json_dict = json.loads(txt)
    return (json_dict['business_id'], json_dict['user_id'])

reviews = sc.textFile('datasets/review.json') \
            .map(preprocess_reviews)

business_reviews = business.join(reviews)

output = ["user_id,business_id"]
output.extend(business_reviews.map( lambda x: f"{x[1][1]},{x[0]}" ).collect())

with open("datasets/ub.csv", "w") as out:
    out.write("\n".join(output))