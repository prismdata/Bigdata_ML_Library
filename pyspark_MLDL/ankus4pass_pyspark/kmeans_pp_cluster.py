#-*- coding: utf-8 -*-

import os

import findspark
findspark.init()

from operator import add
from pyspark import SparkContext
import numpy as np
import pickle
import sys 
import time
import csv

def compute_entropy(d):
    d = np.array(d)
    d = 1.0 * d / d.sum()
    return -np.sum(d * np.log2(d))    

def choise(p):#Generate random sample from 0 to len(p)
    random = np.random.random()   
    r = 0.0
    for idx in range(len(p)):
        r = r + p[idx]
        if r > random:
            return idx

def parse_data(row):
    return (row[0], np.concatenate([row[1], row[2], row[3]]))

def choice(p):
    random = np.random.random()
    r = 0.0
    for idx in range(len(p)):
        r = r + p[idx]
        if r > random:
            return idx
    assert(False)
    
def get_cost(rdd, centers):
    def center_vector_cost(centers, p):
        best = [0] * len(centers)
        closest = [np.inf] * len(centers)
        for idx in range(len(centers)):
            for j in range(len(centers[0])):
                temp_dist = np.linalg.norm(p - centers[idx][j])
                if temp_dist < closest[idx]:
                    closest[idx] = temp_dist
                    best[idx] = j
        return np.array(closest)**2
    
    cost = rdd.map(lambda (name, vector): center_vector_cost(centroids, vector)).collect()
    return np.array(cost).sum(axis=0)


def data_dump(record):
    print record,'\n'
    
def kmeans_init(rdd, K, RUNS, seed):
    '''
    Select `RUNS` sets of initial points for `K`-means++
    '''
    # the `centers` variable is what we want to return
    n_data = rdd.count()
    shape = rdd.take(1)[0][1].shape[0]
    centers = np.zeros((RUNS, K, shape))

    def update_dist(vec, dist, k):
        new_dist = np.linalg.norm(vec - centers[:, k], axis=1)**2
        return np.min([dist, new_dist], axis=0)

    # The second element `dist` in the tuple below is the closest distance from
    # each data point to the selected points in the initial set, where `dist[i]`
    # is the closest distance to the points in the i-th initial set.
    data = rdd.map(lambda p: (p, [np.inf] * RUNS)).cache()
    
    # Collect the feature vectors of all data points beforehand, might be
    # useful in the following for-loop
    local_data = rdd.map(lambda (name, vec): vec).collect()
    
    # Randomly select the first point for every run of k-means++,
    # i.e. randomly select `RUNS` points and add it to the `centers` variable
    sample = [local_data[k] for k in np.random.randint(0, len(local_data), RUNS)]
    centers[:, 0] = sample
    for idx in range(K - 1):
        data = data.map(lambda ((name,vec),dist): ((name,vec),update_dist(vec,dist,idx))).cache()
        
        #Calculate sum of D_i(x)^2
        d1 = data.map(lambda ((name,vec),dist): (1,dist))
        d2 = d1.reduceByKey(lambda x,y: np.sum([x,y], axis=0))
        total = d2.collect()[0][1]
        
        #Normalize each distance to get the probabilities and reshapte to 12140x25
        prob = data.map(lambda ((name,vec),dist): np.divide(dist,total)).collect()
        prob = np.reshape(prob,(len(local_data), RUNS))        
        #K'th centroid for each run
        data_id = [choice(prob[:,i]) for i in xrange(RUNS)]
        sample = [local_data[i] for i in data_id]        
        centers[:, idx+1] = sample        
    return centers

def get_centers(object_point, centers):
    '''
    Return the indices the nearest centroids of "p".
    "centers" contains sets of centroids, where "centers[i]" is   the i-th set of centroids.
    '''
    best = [0] * len(centers)
    closest = [np.inf] * len(centers)
    for idx in range(len(centers)):
        for j in range(len(centers[0])):
            tmp_distence = np.linalg.norm(object_point - centers[idx][j])
            if tmp_distence < closest[idx]:
                closest[idx] = tmp_distence
                best[idx] = j
    return best

def get_closest_centers(object_point, centers):
    center_idx = 0
    closest = [np.inf] * len(centers)
    for idx in range(len(centers)):
        for j in range(len(centers[0])):
            tmp_distence = np.linalg.norm(object_point - centers[idx][j])
            if tmp_distence < closest[idx]:
                closest[idx] = tmp_distence
                center_idx = j
    return center_idx

'''
Run K-means++ algorithm on `rdd`, where `RUNS` is the number of initial sets to use.
'''
def kmeans(rdd, K, RUNS, converge_dist, seed):
  
    k_points = kmeans_init(rdd, K, RUNS, seed)


    temp_dist = 1.0    
    iters = 0
    while temp_dist > converge_dist:       
        #For each point x, select its nearest centroid in i-th centroids set
        cen_rdd1 = rdd.flatMap(lambda each_point: [ ( (indx , j ), each_point[1] ) for (indx,j) in enumerate(get_centers(each_point[1], k_points))])
        
        #Introduce 1 for count
        cen_rdd2 = cen_rdd1.map(lambda ((run, k), pt): ((run, k), np.array([pt, 1])))
                                
        #Add all the distance and add 1's (count)    
        cen_rdd3 = cen_rdd2.reduceByKey(lambda x,y: np.sum([x,y], axis=0))
 
        #Calculate mean distance for each run   
        #Format: ((RUN, nearest_centroid), mean_distance)
        cen_rdd4 = cen_rdd3.map(lambda ((run, k), p):  ((run, k), p[0]/p[1]))
        
        #Get dictionary of new_points
        new_points = cen_rdd4.collectAsMap()
        
        # You can modify this statement as long as `temp_dist` equals to
        # max( sum( l2_norm of the movement of j-th centroid in each centroids set ))
        ##############################################################################
        temp_dist = np.max([
                np.sum([np.linalg.norm(k_points[idx][j] - new_points[(idx, j)]) for idx,j in new_points.keys()])
                            ])

        # update old centroids You modify this for-loop to meet your need
        for ((idx, j), p) in new_points.items():
            k_points[idx][j] = p

    return k_points

import yaml

if __name__ == "__main__":
    yaml_file = open('etc/spark_config_local.yaml', 'r')
    spark_yaml = yaml.load(yaml_file)
    install_path = spark_yaml["spark_env"]
    SPARK_LOCAL_IP = spark_yaml["SPARK_LOCAL_IP"]
    PYSPARK_DRIVER_PYTHON = spark_yaml["PYSPARK_DRIVER_PYTHON"]
    PYSPARK_DRIVER_PYTHON_OPTS = spark_yaml["PYSPARK_DRIVER_PYTHON_OPTS"]

    os.environ["SPARK_HOME"] = install_path
    os.environ["SPARK_LOCAL_IP"] = SPARK_LOCAL_IP
    os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_DRIVER_PYTHON
    os.environ["PYSPARK_DRIVER_PYTHON_OPTS"] = PYSPARK_DRIVER_PYTHON_OPTS

    K = 10
    numbers_of_parallel_kmeans = 20
    converge_dist = 0.03
    bath_path = os.path.dirname(os.path.abspath(__file__))
    input_path = bath_path + '/DataSet/iris.csv'
    output_path = 'iris.pkl'
    if len(sys.argv) == 4:
        K = int(sys.argv[1])
        numbers_of_parallel_kmeans = int(sys.argv[2]) #'병렬로 실핼할 kmeans갯수'
        converge_dist = float(sys.argv[3])
        input_path = bath_path +sys.argv[4]
        output_path = sys.argv[5]

    else:
        print 'Arguments is not valid'
        print 'Run with default argument'
        print 'K', K, 'Parallel Kmeans', numbers_of_parallel_kmeans, ' Converge Dist', converge_dist
        print 'source data', input_path
    seed = 1000

    input_raw_data_list = []
    f = open(input_path, 'r')
    rdr = csv.reader(f)
    for line in rdr:
        if len(line) > 0:
            object_name = ''.join(line[4:5]) #객체명 인덱스 예)4
            input_raw_data_list.append((object_name, np.array(list(map(float, line[0:4])))))#벡터 크기 예)0~3 4개
    f.close()

    sc = SparkContext(appName="pyspark_kmeans_model")
    rdd = sc.parallelize(input_raw_data_list) #'str type object name, numpy vector'

    st = time.time()
    np.random.seed(seed)
    centroids = kmeans(rdd, K, numbers_of_parallel_kmeans, converge_dist, np.random.randint(1000))

    print('K', K)
    print('RUNS', numbers_of_parallel_kmeans)

    cost = get_cost(rdd, centroids)
    log2 = np.log2
    print log2(np.max(cost)), log2(np.min(cost)), log2(np.mean(cost))
    best = np.argmin(cost)

    #dump model as pickle
    with open(output_path, 'wb') as f:
        pickle.dump(centroids[best], f)
    group2 = rdd.map(lambda (in_vector):(in_vector[0], 'cluster:' + str(get_closest_centers(in_vector[1], centroids[best])))).collect()
    for element in group2:
        print element

    sc.stop()



