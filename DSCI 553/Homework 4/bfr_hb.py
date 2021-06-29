from pyspark import SparkContext
from typing import Tuple, List, Dict, Any
import random
import math
import pdb
import os
import sys
import json
from collections import OrderedDict

#python bfr_hb.py data/test4 8 output.json intermediate.csv

def initialize_clusters(datapoints_sample, k) -> Dict[int, List[str]]:
    init = random.sample(datapoints_sample, k)
    centroids = dict((i,k[1]) for i,k in enumerate(init))
    return centroids

def findEuclideanDistance(a, b) -> float:
    dist = 0
    for i,j in zip(a,b):
        dist += (float(i)-float(j))**2
    return math.sqrt(dist)

def assign_cluster(co_ords: List[str], 
            centroids: Dict[int, List[str]]) -> int:
    cluster = -1
    min_dist = float('inf')
    for k,v in centroids.items():
        curr_dist = findEuclideanDistance(co_ords, v)
        if min_dist > curr_dist:
            min_dist = curr_dist
            cluster = k
    return cluster

def calculateAverage(cluster_points: List[int], 
                    dp_map: Dict[str, List[str]], N: int) -> List[float]:
    avg = [0 for _ in range(N)]
    for i in range(N):
        for p in cluster_points:
            avg[i] += float(dp_map[str(p)][i])
        avg[i] /= len(cluster_points) 
    return (avg)

def calculateSumSquare(cluster_points, dp_map, N):
    sumsq = [0 for i in range(N)]
    for i in range(N):
        for p in cluster_points:
            sumsq[i] += float(dp_map[str(p)][i]) ** 2
    return sumsq

def calculateMahalanobis(co_ords, N, stats):
    min_dist = float('inf')
    cluster = -1
    for j, summ in stats.items():
        sum_ = 0
        CNT, SUM, SUMSQ= summ[0], summ[1], summ[2]
        STD = [ math.sqrt( (b/CNT - (a/CNT)**2) )  for a,b in zip(SUM, SUMSQ)  ]
        for i in range(N):    
            mb = float(co_ords[i]) - float(SUM[i])/CNT
            mb = mb/STD[i]
            mb = mb ** 2
            sum_ += mb
        curr_dist = math.sqrt(sum_)
        if min_dist > curr_dist and curr_dist <= 2*math.sqrt(N):
            min_dist = math.sqrt(sum_) 
            cluster = j
    return cluster

def k_means(dp: Any, 
            centroids: Dict[int, List[str]], 
            dp_map: Dict[str, List[str]], n: int) -> Tuple[Any, Dict[int, List[float]]]:
            
    prev_centroids: Dict[int, List[str]] = dict()
    cluster_list: Any = dict()
    max_iteration = 20
    while prev_centroids != centroids and max_iteration != 0:
        cluster_map = dp.mapValues(lambda x : assign_cluster(x, centroids))
        cluster_list = cluster_map.map(lambda x: (x[1], [int(x[0])])).reduceByKey(lambda x,y: x+y)
        #print('i : ', i, '\ncl : ', cluster_list.take(5))
        prev_centroids = centroids
        centroids = cluster_list.mapValues(lambda x: calculateAverage(x, dp_map, n)).collectAsMap()
        max_iteration -= 1
    return cluster_list, centroids

def calculateStatistics(list_of_points: List[int], 
                    map: Dict[str, List[str]], n: int) -> Tuple[int, List[float], List[float]]:
    count = len(list_of_points)
    sum_ = [v*count for v in calculateAverage(list_of_points, map, n)]
    sumsq_ = calculateSumSquare(list_of_points, map, n)
    return ( count, sum_, sumsq_)

path = sys.argv[1]
sc = SparkContext.getOrCreate()
sc.setLogLevel("OFF")
sc.setSystemProperty('spark.driver.memory', '4g')
sc.setSystemProperty('spark.executor.memory', '4g')
files = os.listdir(path)
files.sort()
firstround = True

discard: Dict[str, int] = dict()
rs: Dict[str, int] = dict()
cs: Dict[int, List[str] ] = dict()
cs_op: Dict[str, int] = dict()


K = int(sys.argv[2])
output_file = sys.argv[3]
intermediate_file = sys.argv[4]

round_id = 1
intermediate_headers = ["round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained"]
for f in files:
# read csv and create map between indices and coordinates
    # variable initialization
    threshold = 5
    #init = []
    clusters: Dict[int, List[str]] = dict()
    if firstround:
        firstround = False
        
        datapoints: List[ Tuple[str, List[str]] ] = sc.textFile(path+"/"+f).map(lambda f: f.split(",")) \
                    .map(lambda x: (x[0],x[1:])).collect()

        sample_size = int(0.3 * len(datapoints))
        #random.shuffle(datapoints)   #taking first 10000 points
        datapoints_sample = datapoints[:sample_size]
        datapoints_map = dict(datapoints_sample)
        datapoints_remaining = datapoints[sample_size:]
        # datapoints_remaining_map = dict(datapoints_remaining)
        print('sample size : ', sample_size)
        N = len(datapoints[0][1])
        print('no_of_dimensions: ', N)

        # initialize k points
        centroids = initialize_clusters(datapoints_sample, K*3)
        dp = sc.parallelize(datapoints_sample)

        #cluster_list = RDD[ (index of cluster, list of points' index) ]
        #centroids =  Dict[cluster_index , average value]
        
        cluster_list, centroids = k_means(dp, centroids, datapoints_map, N)  

        #cluster_dict = Dict[ (index of cluster, list of points' index) ]
        cluster_dict = cluster_list.collectAsMap()
        
        #outliers = Dict[ (custer index, list of points' index)]
        outliers = cluster_list.filter(lambda x: len(x[1]) < threshold) \
                .flatMap(lambda x: [j for j in x[1]]) \
                .map(lambda x: (str(x), datapoints_map[str(x)]))

        #interior = Dict[ (custer index, list of points' index)]
        interior = cluster_list.filter(lambda x: len(x[1]) >= threshold) \
                .flatMap(lambda x: [j for j in x[1]]) \
                .map(lambda x: (str(x), datapoints_map[str(x)]))

        #interior_map = interior.collectAsMap()
        interior_list = interior.collect()

        #centroids = Dict[ (custer index, list of co-ords)],10,42612,0,

        centroids = initialize_clusters(interior_list, K)

        #discard_set_clusters = RDD[ (index of cluster, list of points' index) ]
        discard_set_clusters, centroids_int = k_means(interior, centroids, datapoints_map, N)
        discard_set = discard_set_clusters.collectAsMap()
        # print('discard set : ', discard_set_clusters.take(2))

        #discard set op
        ds_op = discard_set_clusters.flatMap(lambda x: [(str(j),x[0]) for j in x[1]])\
                .collectAsMap()
        discard.update(ds_op)
        # print('discard : ', len(discard))
        # for k,v in discard_set.items():
        #     print('cluster index : ', k, ' no of points : ', len(v))
        # print('no of discarded points: ', len(ds_op))
        #discard_stats = Dict[ (Cluster Index, ( len(list_of_points), sum_, sumsq_, std_dev ) ) ]
        discard_stats = discard_set_clusters.mapValues(lambda x: calculateStatistics(x, datapoints_map, N)).collectAsMap()
        print([v[0] for v in discard_stats.values() ])
        outliers_cnt = outliers.count()
        # print('outliers count : ', outliers.count())
        cs_stats = dict()
        if K*3 < outliers_cnt:
            outliers_map = outliers.collectAsMap()
            outliers_list = outliers.collect()
            centroids = initialize_clusters(outliers_list, K*3)

            #clusters = RDD[ (index of cluster, list of points' index) ]
            clusters, centroids = k_means(outliers, centroids, outliers_map, N)
            
            cluster_map = clusters.collectAsMap()
            
            rs_op = clusters.filter(lambda x: len(x[1]) < 2) \
                    .flatMap(lambda x: [(str(y),-1) for y in x[1]] ).collectAsMap()
            
            rs.update(rs_op)

            cs = clusters.filter(lambda x: len(x[1]) > 1) 
            cs_map = cs.collectAsMap()
            
            cs_stats = cs.mapValues(lambda x: calculateStatistics(x, datapoints_map, N)).collectAsMap()
            
        else:
            rs.update(outliers.mapValues(lambda x: -1).collectAsMap())
    else:
        datapoints_remaining = sc.textFile(path+"/"+f).map(lambda f: f.split(",")) \
                    .map(lambda x: (x[0],x[1:])).collect()
   
    datapoints_remaining_map = dict(datapoints_remaining)
    dp_remaining = sc.parallelize(datapoints_remaining)
    
    all_clusters = dp_remaining.mapValues(lambda x: calculateMahalanobis(x, N, discard_stats))
    assigned = all_clusters.filter(lambda x: x[1] >= 0)
    ds_out = assigned.collectAsMap()
    discard.update(ds_out)
    unassigned = all_clusters.filter(lambda x: x[1] < 0)
    
    if len(cs_stats) > 0:
        all_cs = unassigned.map(lambda x: (x[0], calculateMahalanobis(datapoints_remaining_map[str(x[0])], N, cs_stats)))
        assigned_cs = all_cs.filter(lambda x: x[1] >= 0).collectAsMap()
        unassigned_cs = all_cs.filter(lambda x: x[1] < 0).map(lambda x: (x[0], -1)).collectAsMap()
        rs.update(unassigned_cs)
        rs.update(assigned_cs)
    else:
        rs.update(unassigned.collectAsMap())

    if cs: 
        cs_to_ds = cs.map(lambda x: (x[0], calculateMahalanobis(
            [y/cs_stats[x[0]][0] for y in cs_stats[x[0]][1]]
            , N, discard_stats)) )
        cs = cs_to_ds.filter(lambda x: x[1] < 0)
        cs_merged_ds = cs_to_ds.filter(lambda x: x[1] >= 0) \
            .flatMap(lambda x: [(str(y),x[1]) for y in cs_map[x[0]]]).collectAsMap()
        discard.update(cs_merged_ds)
    intermediate_headers.append(f"{round_id},{len(discard_stats)},{len(discard)},{len(cs_op)},{len(cs_op)},{len(rs)}")

discard.update(rs)
if cs_op: discard.update(cs_op)

with open(output_file, 'w') as out:
    answer = OrderedDict()
    for k in sorted(discard.keys(), key=lambda x: int(x)):
        answer[str(k)] = discard[str(k)]
    out.write(json.dumps(answer))

with open(intermediate_file, "w") as op:
    op.write("\n".join(intermediate_headers))