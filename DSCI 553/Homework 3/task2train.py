from pyspark import SparkContext
import json
import re
from collections import defaultdict
import math
import sys

#spark-submit task2train.py $ASNLIB/publicdata/train_review.json task2.model $ASNLIB/publicdata/stopwords
args = sys.argv

input_file_path = args[1]
model_file = args[2]
stopwords_file = args[3]

sc = SparkContext("local[*]", "homework3_task2")

stopwords = set(sc.textFile(stopwords_file).map(lambda x: x.strip()).collect())
stopwords.add("")

def preprocess(json_text):
    json_dict = json.loads(json_text)
    
    text = re.sub(r'[\(\[,\].!?:;\)\-&*%$0-9\'~]', " ", json_dict['text'].lower() ).split()
    json_dict['text'] = [ word for word in text if word not in stopwords ]
    
    return ( json_dict['business_id'], json_dict['text'] )

business_review = sc.textFile(input_file_path) \
        .map(preprocess) \
        .reduceByKey(lambda x,y : x+y)

TOTAL_DOCS = business_review.count()

wordCount = business_review.flatMap( lambda x: [(w,1) for w in x[1]] ) \
    .reduceByKey(lambda x,y: x+y)

TOTAL_NO_OF_WORDS = wordCount.map( lambda x: x[1] ).reduce(lambda x,y: x+y)

rarewords = set(wordCount.filter(lambda x: x[1] < TOTAL_NO_OF_WORDS*0.000001) \
        .map(lambda x: x[0]).collect() )


business_review = business_review.mapValues( 
    lambda words: [word for word in words if word not in rarewords] )


def term_frequency(words):
    count = defaultdict(int)
    for word in words: count[word]+=1
    mx_freq = max(count.values())
    for k,v in count.items(): count[k] = v/mx_freq
    return list(count.items())

term_freq = business_review.mapValues(term_frequency)
idf = business_review.flatMap( lambda x:  [ (w,1) for w in set(x[1]) ] ) \
        .reduceByKey( lambda x,y: x+y ) \
        .mapValues( lambda x: math.log2(TOTAL_DOCS/x) ) \
        .collectAsMap()
        

def calc_tfidf(x, idf):
    tfidf = []
    for word, freq in x:
        tfidf.append( ( freq*idf[word], word) )
    tfidf.sort(reverse=True)
    return list(map( lambda x: x[1], tfidf[:200] ))

tfidf = term_freq.mapValues( lambda x: calc_tfidf(x, idf))


words = tfidf.flatMap(lambda x: x[1]).distinct().collect()
word2idx = { word:idx for word,idx in zip(words, range(len(words))) }


def replaceWithIndex(words, w2i):
    ans = set()
    for word in words: ans.add( w2i[word] )
    return ans

documents = tfidf.mapValues( lambda x: replaceWithIndex(x, word2idx) )
documentProfileMap = documents.collectAsMap()

def preprocess_user_bussiness(x):
    json_dict = json.loads(x)
    return ( json_dict['user_id'], {json_dict['business_id']} )

def createUserProfile(values, documentProfile):
    userProfile = set()
    for bid in values: userProfile = userProfile | documentProfile[bid]
    return userProfile

with open(model_file, "w") as out:

    documentProfile = documents.map(lambda x: "{"+f'"type":"Document", "id":"{x[0]}", "value":{list(x[1])}'+"}") \
        .collect()
    out.write("\n".join(documentProfile))
    out.write("\n")

    userProfile = sc.textFile( input_file_path ) \
        .map(preprocess_user_bussiness) \
        .reduceByKey( lambda x,y: x|y ) \
        .mapValues( lambda values: createUserProfile(values, documentProfileMap) ) \
        .map(lambda x: "{"+f'"type":"User", "id":"{x[0]}", "value":{list(x[1])}'+"}") \
        .collect()

    out.write("\n".join(userProfile))