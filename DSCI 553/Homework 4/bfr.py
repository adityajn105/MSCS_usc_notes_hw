from time import clock_settime
from pyspark import SparkContext
import os
import random
from itertools import combinations
import math
import json
import sys
from time import time
# os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

sc = SparkContext.getOrCreate()

input_dir = sys.argv[1]
n_clusters = int(sys.argv[2])
intermediate_results = sys.argv[3]
output_file = sys.argv[4]

#random.seed(10)

alpha = 3
round = 1
files = list(map(lambda x: os.path.join(input_dir,x), os.listdir(input_dir)))
files.sort( key=lambda x: x)

ans = dict()
discard_set = dict()
compression_set = dict()
retained_set = []

intermediates = [ "round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained"]   

def getDataLoad(file):
    data = []
    fp = open(file, "r")
    for line in fp.readlines():
        points = line.split(',')
        data.append([float(x) for x in points])
    fp.close()
    #random.shuffle(data)
    return data

def euclidean(x1, x2):
    ans = 0
    for i1, i2 in zip(x1,x2):
        ans += (i1-i2)**2
    return math.sqrt(ans)

def argmin(x):
    n = len(x)
    min_x, min_i = float('inf'), 0
    for i, xi in zip(range(n), x):
        if xi < min_x: min_x, min_i = xi, i
    return min_i

def add_vector( v1, v2 ):
    v = []
    for i,j in zip(v1,v2): v.append(i+j)
    return v

data = getDataLoad(files[0])
d = len(data[0])-1
threshold = 3*math.sqrt(d)

class KMeans():
    def __init__(self, n_clusters=10, max_iterations=20):
        self.k = n_clusters
        self.max_it = max_iterations
    
    def cluster_changed(self, old, new):
        for o,n in zip(old, new):
            if o!=n: return True
        return False
    
    def initialize_cluster(self, x):
        centers = random.sample(x, self.k)
        return centers
    
    # def initialize_cluster(self, x):
    #     cluster_centers = random.sample( x, 1 )
    #     for i in range(1, self.k):
    #         dist, idx = 0, 0
    #         for j in range(len(x)):
    #             curr = 0
    #             for k in range(i):
    #                 curr += euclidean( x[j], cluster_centers[k]  )
    #             if curr > dist: dist, idx = curr, j
    #         cluster_centers.append(x[idx])
    #     return cluster_centers

    def fit(self, data):
        "returns ans, sumamry, Map[id, cluster_id], "
        initial = sc.parallelize(data).map(lambda x: ( str(int(x[0])), x[1:] ))
        cluster_centers = self.initialize_cluster([row[1:] for row in data])
        i = 0
        while i != self.max_it:
            point_cluster = initial.mapValues( lambda x: [euclidean(x, center) for center in cluster_centers] ) \
                             .mapValues(lambda x: argmin(x)).collectAsMap() #(id, cluster_id)

            new_cluster_centers = initial.map(lambda x: (point_cluster[x[0]], (x[1],1)) ) \
                            .reduceByKey( lambda x,y: (add_vector(x[0],y[0]), x[1]+y[1]) ) \
                            .mapValues( lambda x: [y/x[1] for y in x[0]] )

            new_cluster_centers = [ x[1] for x in sorted(new_cluster_centers.collect()) ]

            if self.cluster_changed(cluster_centers, new_cluster_centers):
                cluster_centers = new_cluster_centers
            else: 
                cluster_centers = new_cluster_centers 
                break
            i+=1

        summary = initial.mapValues( lambda x: ([euclidean(x, center) for center in cluster_centers], x) ) \
                        .map( lambda x: (argmin(x[1][0]), (1, x[1][1], [v**2 for v in x[1][1]])) ) \
                        .reduceByKey( lambda x,y: [x[0]+y[0], add_vector(x[1],y[1]), add_vector(x[2],y[2])] ) \
                        .collectAsMap() 
        
        cluster_points = initial.map( 
            lambda x: (argmin([euclidean(x[1], center) for center in cluster_centers]), (x[0], x[1]) ) ) \
            .groupByKey().mapValues(list).collectAsMap()
        
        return point_cluster, summary, cluster_points

def seperate_retained( ans, summary,  cluster_points):
    retained = []
    for k,v in list(summary.items()):
        if v[0] <= 1:
            if v[0] == 1:
                key, val = cluster_points[k][0]
                retained.append(  [float(key)]+val ) 
                ans.pop(key)
            summary.pop(k)
            cluster_points.pop(k)
    return ans, summary, cluster_points, retained

def getInteriorRetained( cluster_points, t=10):
    interior, retained = [], []
    for _, ls in cluster_points.items():
        if len(ls) < t:
            for key, val in ls: retained.append( [float(key)]+val )
        else:
            for key, val in ls: interior.append( [float(key)]+val )
    return interior, retained

start = time()
fraction = int(len(data)*0.3)
sample = data[:fraction]

ans, summary, cluster_points = KMeans(n_clusters = n_clusters*5).fit(sample)

interior, retained = getInteriorRetained(cluster_points)
retained_set.extend(retained)

ds_ans, ds_summary, _ = KMeans(n_clusters = n_clusters, max_iterations=5).fit(interior)

rest_data = data[fraction:]
point_cluster, summary, cluster_points = KMeans(n_clusters = n_clusters*3, max_iterations = 5).fit(rest_data)

cs_map, cs_summary, _, retain = seperate_retained(point_cluster, summary, cluster_points)
retained_set.extend(retain)

print( f"round_id: {round}, no_of_cluster_discard: {len(ds_summary)}, no_of_points_discard: {len(ds_ans)}, no_of_cluster_compression: {len(cs_summary)}, no_of_points_compression: {len(cs_map)}, no_of_points_retained: {len(retained_set)}"  )
intermediates.append(f"{round},{len(ds_summary)},{len(ds_ans)},{len(cs_summary)},{len(cs_map)},{len(retained_set)}")

def mahalanobis_distance( point, N, SUM, SUMSQ ):
    mh = 0
    for i in range(d):
        std = math.sqrt( (SUMSQ[i]/N) - (SUM[i]/N)**2 )
        centroid = SUM[i]/N
        normalized = (point[i]-centroid)/std
        mh += (normalized**2)
    return math.sqrt(mh)

def assign_to_cluster( point, threshold, summary ):
    min_idx, min_mh = 0, float('inf')
    for idx, summ in summary.items():
        N, SUM, SUMSQ = summ[0], summ[1], summ[2]
        mh = mahalanobis_distance(point, N, SUM, SUMSQ)
        if mh < min_mh: min_mh, min_idx = mh, idx
            
    if min_mh < threshold: return min_idx
    else: return -1
    
def updateSummary(old_sum, updates):
    for idx, summary in updates.items():
        old_sum[idx][0] += summary[0]
        for i in range(d):
            old_sum[idx][1][i] += summary[1][i]
            old_sum[idx][2][i] += summary[2][i]


def updateCS( cs_ans, cs_summary, point_cluster, summary ):
    mx_idx = max(cs_summary.keys())
    for pt, cluster in point_cluster.items():
        cs_ans.update( { pt: cluster+mx_idx } )
    
    for cluster, summary in summary.items():
        cs_summary.update( { cluster+mx_idx: summary } )


for file in files[1:]:
    data = getDataLoad(file)
    rdd = sc.parallelize(data).map( lambda x: (str(int(x[0])), x[1:]) ) \
        .map(lambda x: (assign_to_cluster(x[1], threshold, ds_summary), x[0], x[1] ) )

    ds_ans.update(rdd.filter(lambda x: x[0] != -1).map( lambda x: (x[1], x[0]) ).collectAsMap())
    
    
    ds_updates = rdd.filter( lambda x: x[0] != -1 ).map(lambda x: (x[0], (1, x[2], [v**2 for v in x[2]]) ) ) \
        .reduceByKey( lambda x,y: ( x[0]+y[0], add_vector(x[1],y[1]), add_vector(x[2],y[2]) ) ) \
        .collectAsMap()

    updateSummary(ds_summary, ds_updates)
    cs_rdd = rdd.filter( lambda x: x[0] == -1 ).map( lambda x: (x[1], x[2]) ) \
        .map(lambda x: (assign_to_cluster(x[1], threshold, cs_summary), x[0], x[1] ) )
    
    cs_map.update(cs_rdd.filter(lambda x: x[0] != -1).map( lambda x: (x[1], x[0]) ).collectAsMap())

    cs_updates = cs_rdd.filter( lambda x: x[0] != -1 ).map(lambda x: (x[0], (1, x[2], [v**2 for v in x[2]]) ) ) \
        .reduceByKey( lambda x,y: ( x[0]+y[0], add_vector(x[1],y[1]), add_vector(x[2],y[2]) ) ) \
        .collectAsMap()

    updateSummary(cs_summary, cs_updates)

    retained_set.extend(cs_rdd.filter( lambda x: x[0] == -1 )\
                    .map( lambda x: [float(x[1])]+x[2] ).collect())



    # if len(retained_set) >= 3*n_clusters:
    #     point_cluster, summary, cluster_points = KMeans( n_clusters=3*n_clusters)\
    #         .fit(retained_set)
    #     point_cluster, summary, _, retained_set = seperate_retained(point_cluster, summary, cluster_points)
        
    #     updateCS( cs_map, cs_summary, point_cluster, summary )

    # avail = set(cs_summary.keys())
    # new_cluster_map = {}
    # i = 0
    # for key1, key2 in combinations(cs_summary.keys(), 2):
    #     if key1 in avail and key2 in avail:
    #         center2 = [ v/cs_summary[key2][0] for v in cs_summary[key2][1]]
    #         mh = mahalanobis_distance( center2, \
    #             cs_summary[key1][0], cs_summary[key1][1], cs_summary[key1][2] )
    #         if mh < threshold:
    #             new_cluster_map[key1] = i
    #             new_cluster_map[key2] = i
    #             avail.discard(key1)
    #             avail.discard(key2)
    #             i+=1
    # for key in cs_summary.keys():
    #     if key not in new_cluster_map:
    #         new_cluster_map[key] = i
    #         i+=1
                
    # for idx,old_cluster in cs_map.items():
    #     cs_map[idx] = new_cluster_map[old_cluster]

    # temp_cs_summary = deepcopy(cs_summary)
    # cs_summary.clear()
    # for old, new in new_cluster_map.items():
    #     if new not in cs_summary:
    #         cs_summary[new] = temp_cs_summary[old]
    #     else:
    #         updateSummary( cs_summary, { new: temp_cs_summary[old] } )
    round+=1
    print( f"round_id: {round}, no_of_cluster_discard: {len(ds_summary)}, no_of_points_discard: {len(ds_ans)}, no_of_cluster_compression: {len(cs_summary)}, no_of_points_compression: {len(cs_map)}, no_of_points_retained: {len(retained_set)}"  )
    intermediates.append(f"{round},{len(ds_summary)},{len(ds_ans)},{len(cs_summary)},{len(cs_map)},{len(retained_set)}")

new_cluster_map = dict()
for key, summary in cs_summary.items():
    center = [ v/summary[0] for v in summary[1]]
    best = (float('inf'), 0)
    for ds_cluster_idx, ds_sum in ds_summary.items():
        mh = mahalanobis_distance( center, ds_sum[0], ds_sum[1], ds_sum[2] )
        if mh < best[0]: best = (mh, ds_cluster_idx)
    new_cluster_map[key] = best[1]
    
for idx, old_cluster in cs_map.items():
    cs_map[idx] = new_cluster_map[old_cluster]
ds_ans.update(cs_map)
    
centers = []
for i in range(len(ds_summary)):
    summary = ds_summary[i]
    center = [ v/ds_summary[i][0] for v in ds_summary[i][1] ]
    centers.append(center)

ds_ans.update(sc.parallelize( retained_set ).map( lambda x: (str(int(x[0])), x[1:]) ) \
    .mapValues( lambda x: -1 ) \
    .collectAsMap())

round+=1
print( f"round_id: {round}, no_of_cluster_discard: {len(ds_summary)}, no_of_points_discard: {len(ds_ans)}, no_of_cluster_compression: {len(cs_summary)}, no_of_points_compression: {len(cs_map)}, no_of_points_retained: {len(retained_set)}"  )
intermediates.append(f"{round},{len(ds_summary)},{len(ds_ans)},{len(cs_summary)},{len(cs_map)},{len(retained_set)}")
with open(intermediate_results, "w") as inter:
    print("\n".join(intermediates), file=inter)

with open(output_file, "w") as out:
    output = json.dumps(ds_ans, sort_keys=True)
    print(output, file=out)

print(f"Time Taken : {time()-start}")