from pyspark import SparkContext
import json
import sys
import random
from itertools import combinations
from collections import OrderedDict
import math
import os

#os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
#os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

input_dir = sys.argv[1]
n_clusters = int(sys.argv[2])
output_file = sys.argv[3]
intermediate_results = sys.argv[4]

sc = SparkContext.getOrCreate()
sc.setLogLevel("OFF")
sc.setSystemProperty('spark.driver.memory', '4g')
sc.setSystemProperty('spark.executor.memory', '4g')

alpha = 2.5
no_of_rounds_completed = 1
list_of_files = list(sorted(map(lambda x: os.path.join(input_dir,x), os.listdir(input_dir))))

retained_points = []
ans = {}
compression_stats = {}
discard_stats = {}

def euclidean_distance_between_vectors(x1, x2): return math.sqrt(sum([(i1-i2)**2 for i1,i2 in zip(x1,x2)]))

def min_argument_in_list(x):
    n = len(x)
    min_x, min_i = float('inf'), 0
    for i, xi in zip(range(n), x):
        if xi < min_x: min_x, min_i = xi, i
    return min_i

def vector_addition( v1, v2 ): return [ i+j for i,j in zip(v1,v2) ]

def getdataListLoad(file_path):
    dataList = list()    
    with open(file_path, "r") as fp:
        for line in fp.readlines():
            points = line.split(',')
            dataList.append([float(x) for x in points])
    random.shuffle(dataList)
    return dataList


log_list = [ "no_of_rounds_completed,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained"]   

dataList = getdataListLoad(list_of_files[0])

d = len(dataList[0])-1
tres_hold = alpha*math.sqrt(d)

class k_means_algo():
    def __init__(self, n_clusters=10, max_iterations=30):
        self.k = n_clusters
        self.max_it = max_iterations
    
    def startingize_cluster_center(self, x):
        return random.sample(x, self.k)

    def fit(self, dataList, cluster_centers = None):
        "returns ans, sumamry, Map[id, cluster_id], "
        starting = sc.parallelize(dataList).map(lambda x: ( str(int(x[0])), x[1:] ))
        if cluster_centers == None:
            cluster_centers = self.startingize_cluster_center([row[1:] for row in dataList])
        i = 0
        while i != self.max_it:
            points_in_clusters = starting.mapValues( lambda x: [euclidean_distance_between_vectors(x, center) for center in cluster_centers] ) \
                             .mapValues(lambda x: min_argument_in_list(x)).collectAsMap() #(id, cluster_id)

            new_cluster_centers = starting.map(lambda x: (points_in_clusters[x[0]], (x[1],1)) ) \
                            .reduceByKey( lambda x,y: (vector_addition(x[0],y[0]), x[1]+y[1]) ) \
                            .mapValues( lambda x: [y/x[1] for y in x[0]] )

            new_cluster_centers = [ x[1] for x in sorted(new_cluster_centers.collect()) ]

            if self.cluster_changed(cluster_centers, new_cluster_centers):
                cluster_centers = new_cluster_centers
            else: 
                cluster_centers = new_cluster_centers 
                break
            i+=1

        
        cluster_points = starting.map( 
            lambda x: (min_argument_in_list([euclidean_distance_between_vectors(x[1], c) for c in cluster_centers]), (x[0], x[1]) ) ) \
            .groupByKey().mapValues(list).collectAsMap()
        
        statistics = starting.mapValues( lambda x: ([euclidean_distance_between_vectors(x, c) for c in cluster_centers], x) ) \
                        .map( lambda x: (min_argument_in_list(x[1][0]), (1, x[1][1], [v**2 for v in x[1][1]])) ) \
                        .reduceByKey( lambda x,y: [x[0]+y[0], vector_addition(x[1],y[1]), vector_addition(x[2],y[2])] ) \
                        .collectAsMap() 
        
        return points_in_clusters, statistics, cluster_points
    
    def cluster_changed(self, old_vec, new_vec):
        for oi,ni in zip(old_vec, new_vec):
            if oi!=ni: return True
        return False

        
def seperate_interior_and_retained( points_in_cluster, t=10):
    interior, retained = [], []
    for _, ls in points_in_cluster.items():
        if len(ls) < t: 
            for key, val in ls: retained.append( [float(key)]+val )
        else:
            for key, val in ls: interior.append( [float(key)]+val )
    return interior, retained

def get_points_to_retain( ans, statistics,  points_in_cluster):
    rs = []
    for k,v in list(statistics.items()):
        if v[0] <= 1:
            if v[0] == 1:
                key, val = points_in_cluster[k][0]
                rs.append(  [float(key)]+val ) 
                ans.pop(key)
            statistics.pop(k)
            points_in_cluster.pop(k)
    return ans, statistics, points_in_cluster, rs


fraction = int(len(dataList)*0.5)
random_sample = dataList[:fraction]

ans, statistics, cluster_points = k_means_algo(n_clusters = n_clusters*5).fit(random_sample)
interior, retained = seperate_interior_and_retained(cluster_points)
retained_points.extend(retained)

discard_answer, discard_set_statistics, _ = k_means_algo(n_clusters = n_clusters).fit(interior)

rest_dataList = dataList[fraction:]
points_in_clusters, statistics, cluster_points = k_means_algo(n_clusters = n_clusters*3, max_iterations = 5).fit(rest_dataList)

cs_map, cs_statistics, _, retain = get_points_to_retain(points_in_clusters, statistics, cluster_points)
retained_points.extend(retain)

log_list.append(f"{no_of_rounds_completed},{len(discard_set_statistics)},{len(discard_answer)},{len(cs_statistics)},{len(cs_map)},{len(retained_points)}")

def Update_CS_OR_DS(old_summation, updates):
    for idx, statistics in updates.items():
        old_summation[idx][0] += statistics[0]
        for i in range(d):
            old_summation[idx][1][i] += statistics[1][i]
            old_summation[idx][2][i] += statistics[2][i]

def ass_2_cluster( point, tres_hold, statistics ):
    min_idx, min_mh = 0, float('inf')
    for idx, summ in statistics.items():
        N, summation, summation_square = summ[0], summ[1], summ[2]
        mh = mh_distance(point, N, summation, summation_square)
        if mh < min_mh: min_mh, min_idx = mh, idx
            
    if min_mh < tres_hold: return min_idx
    else: return -1

def mh_distance( point, N, summation, summation_square ):
    mh = 0
    for i in range(d):
        std = math.sqrt( (summation_square[i]/N) - (summation[i]/N)**2 )
        centroid_point = summation[i]/N
        norm = (point[i]-centroid_point) if std==0 else (point[i]-centroid_point)/std
        mh += math.pow(norm,2)
    return mh**0.5



def updateCS( cs_ans, cs_statistics, points_in_clusters, statistics ):
    mx_idx = max(cs_statistics.keys())
    for pt, cluster in points_in_clusters.items():
        cs_ans.update( { pt: cluster+mx_idx } )
    
    for cluster, summ in statistics.items():
        cs_statistics.update( { cluster+mx_idx: summ } )

def mergeCS( mapp, statistics ):
    new_statistics, new_cmap = dict(), dict()
    keys = list(statistics.keys())
    i = 0
    avail = set(keys)
    for k1, k2 in combinations(keys, 2):
        if k1 not in avail or k2 not in avail: continue
        centroid1 = [ v/statistics[k1][0] for v in statistics[k1][1] ]
        centroid2 = [ v/statistics[k2][0] for v in statistics[k2][1] ]
        mh = max( mh_distance( centroid1, statistics[k2][0], statistics[k2][1], statistics[k2][2] ),
                  mh_distance( centroid2, statistics[k1][0], statistics[k1][1], statistics[k1][2] ) )

        if mh < tres_hold:
            new_cmap[k1], new_cmap[k2] = i, i
            avail.discard(k1)
            avail.discard(k2)
            new_statistics[i] = [ statistics[k1][0]+statistics[k2][0], vector_addition(statistics[k1][1], statistics[k2][1]), vector_addition(statistics[k1][2], statistics[k2][2]) ]
            i+=1
    
    for k,v in mapp.items():
        if v not in new_cmap:
            new_cmap[v] = i
            new_statistics[i] = statistics[v]
            i+=1
        mapp[k] = new_cmap[v]

    return mapp, new_statistics            


for file in list_of_files[1:]:
    dataList = getdataListLoad(file)
    rdd = sc.parallelize(dataList).map( lambda x: (str(int(x[0])), x[1:]) ) \
        .map(lambda x: (ass_2_cluster(x[1], tres_hold, discard_set_statistics), x[0], x[1] ) )

    discard_answer.update(rdd.filter(lambda x: x[0] != -1).map( lambda x: (x[1], x[0]) ).collectAsMap())
    
    
    ds_updates = rdd.filter( lambda x: x[0] != -1 ).map(lambda x: (x[0], (1, x[2], [v**2 for v in x[2]]) ) ) \
        .reduceByKey( lambda x,y: ( x[0]+y[0], vector_addition(x[1],y[1]), vector_addition(x[2],y[2]) ) ) \
        .collectAsMap()
    
    cs_rdd = rdd.filter( lambda x: x[0] == -1 ).map( lambda x: (x[1], x[2]) ) \
        .map(lambda x: (ass_2_cluster(x[1], tres_hold, cs_statistics), x[0], x[1] ) )
    
    cs_map.update(cs_rdd.filter(lambda x: x[0] != -1).map( lambda x: (x[1], x[0]) ).collectAsMap())

    cs_updates = cs_rdd.filter( lambda x: x[0] != -1 ).map(lambda x: (x[0], (1, x[2], [v**2 for v in x[2]]) ) ) \
        .reduceByKey( lambda x,y: ( x[0]+y[0], vector_addition(x[1],y[1]), vector_addition(x[2],y[2]) ) ) \
        .collectAsMap()

    retained_points.extend(cs_rdd.filter( lambda x: x[0] == -1 )\
                    .map( lambda x: [float(x[1])]+x[2] ).collect())
    Update_CS_OR_DS(discard_set_statistics, ds_updates)
    Update_CS_OR_DS(cs_statistics, cs_updates)

    if len(retained_points) >= 3*n_clusters:
        points_in_clusters, statistics, points_in_cluster = k_means_algo( n_clusters=3*n_clusters).fit(retained_points)
        points_in_clusters, statistics, _, retained_points = get_points_to_retain(points_in_clusters, statistics, points_in_cluster)
        updateCS( cs_map, cs_statistics, points_in_clusters, statistics )

    cs_map, cs_statistics = mergeCS( cs_map, cs_statistics )

    no_of_rounds_completed+=1
    log_list.append(f"{no_of_rounds_completed},{len(discard_set_statistics)},{len(discard_answer)},{len(cs_statistics)},{len(cs_map)},{len(retained_points)}")

new_cluster_map = dict()
for key, statistics in cs_statistics.items():
    center = [ v/statistics[0] for v in statistics[1]]
    best = (float('inf'), 0)
    for ds_cluster_idx, ds_sum in discard_set_statistics.items():
        mh = mh_distance( center, ds_sum[0], ds_sum[1], ds_sum[2] )
        if mh < best[0]: best = (mh, ds_cluster_idx)
    new_cluster_map[key] = best[1]
    
for idx, old_cluster in cs_map.items():
    cs_map[idx] = new_cluster_map[old_cluster]

discard_answer.update(cs_map)
    
discard_answer.update(sc.parallelize( retained_points ).map( lambda x: (str(int(x[0])), x[1:]) ) \
    .mapValues( lambda x: -1 ) \
    .collectAsMap())

centers = []
for i in range(len(discard_set_statistics)):
    statistics = discard_set_statistics[i]
    center = [ v/discard_set_statistics[i][0] for v in discard_set_statistics[i][1] ]
    centers.append(center)

out = open(output_file, "w")
answer = OrderedDict()
for k in sorted(discard_answer.keys(), key=lambda x: int(x)):
    answer[str(k)] = discard_answer[str(k)]
out.write(json.dumps(answer))
out.close()


fp = open(intermediate_results, "w")
fp.write("\n".join(log_list))
fp.close()