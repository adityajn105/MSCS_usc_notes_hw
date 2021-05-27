from pyspark import SparkContext
import json
import datetime
import sys

from operator import add 

result = dict()
args = sys.argv

# python task1.py "datasets/review.json" "output.json" "datasets/stopwords" 2017 5 10
sc = SparkContext.getOrCreate()

stopwords = set()
with open(args[3], "r") as fp:
    for word in fp.readlines():
        stopwords.add(word.strip().lower())
stopwords.add("")

puncts = set(["(", "[", ",", ".", "!", "?", ":", ";", "]", ")"])

reviews = sc.textFile(args[1])
def preprocess(json_txt):
    json_dict = json.loads(json_txt)
    json_dict['date'] = datetime.datetime.strptime(json_dict['date'], "%Y-%m-%d %H:%M:%S")
    text = []
    for c in list(json_dict['text'].lower().replace("\n","")):
        if c not in puncts: text.append(c)
    text = "".join(text).split(" ")
    json_dict['text'] = [ word for word in text if word not in stopwords ]
    return json_dict

reviews_json = reviews.map( lambda x : preprocess(x) )

# The total number of reviews
result['A'] = reviews.count()

# The number of reviews in a given year, y
result['B'] = reviews_json.filter( lambda x : x['date'].year == int(args[4])  ).count()

# The number of distinct users who have written the reviews
result['C'] = reviews_json.map( lambda x: (x['user_id'],1) ).reduceByKey(add).count()

# Top m users who have the largest number of reviews and its count
result['D'] = reviews_json \
                .map( lambda x: (x['user_id'],1) ) \
                .reduceByKey(add) \
                .sortBy( lambda x: -1*x[1] ) \
                .take( int(args[5]))

# Top n frequent words in the review text. The words should be in lower cases. 
# The following punctuations “(”, “[”, “,”, “.”, “!”, “?”, “:”, “;”, “]”, “)” and the given stopwords are excluded (1pts)
result['E'] = reviews_json \
            .flatMap( lambda x: [ (word, 1) for word in x['text'] ] ) \
            .reduceByKey(add) \
            .sortBy( lambda x: -1*x[1] ) \
            .map( lambda x: x[0] ) \
            .take( int(args[6]) )

with open( args[2], "w" ) as fp:
    json.dump( result, fp )


# spark-submit task1.py $ASNLIB/publicdata/review.json task1_ans $ASNLIB/publicdata/stopwords 2018 10 10