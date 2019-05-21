#-*- coding: utf-8 -*-

import os

import findspark
findspark.init()

from pyspark import SparkContext
import numpy as np
import pickle
import sys 
import time
import csv
import pandas as pd

class kmeans_pp:

    def compute_entropy(self, d):
        d = np.array(d)
        d = 1.0 * d / d.sum()
        return -np.sum(d * np.log2(d))

    def choise(self, p):
        random = np.random.random()
        r = 0.0
        for idx in range(len(p)):
            r = r + p[idx]
            if r > random:
                return idx

    def parse_data(self, row):
        return (row[0], np.concatenate([row[1], row[2], row[3]]))

    def choice(self,p):
        random = np.random.random()
        r = 0.0
        for idx in range(len(p)):
            r = r + p[idx]
            if r > random:
                return idx
        assert(False)

    def get_cost(self,rdd, centers):
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

        cost = rdd.map(lambda (name, vector): center_vector_cost(self.centroids, vector)).collect()
        return np.array(cost).sum(axis=0)


    def data_dump(self,record):
        print record,'\n'

    def kmeans_init(self, rdd, K, RUNS, seed):
        n_data = rdd.count()
        shape = rdd.take(1)[0][1].shape[0]
        centers = np.zeros((RUNS, K, shape))

        def update_dist(vec, dist, k):
            new_dist = np.linalg.norm(vec - centers[:, k], axis=1)**2
            return np.min([dist, new_dist], axis=0)

        data = rdd.map(lambda p: (p, [np.inf] * RUNS)).cache()

        local_data = rdd.map(lambda (name, vec): vec).collect()

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
            data_id = [self.choice(prob[:,i]) for i in xrange(RUNS)]
            sample = [local_data[i] for i in data_id]
            centers[:, idx+1] = sample
        return centers

    def get_centers(self,object_point, centers):
        best = [0] * len(centers)
        closest = [np.inf] * len(centers)
        for idx in range(len(centers)):
            for j in range(len(centers[0])):
                tmp_distence = np.linalg.norm(object_point - centers[idx][j])
                if tmp_distence < closest[idx]:
                    closest[idx] = tmp_distence
                    best[idx] = j
        return best

    def get_closest_centers(self,object_point, centers):
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
    def fit(self, rdd, K, RUNS, converge_dist, seed):

        k_points = self.kmeans_init(rdd, K, RUNS, seed)

        temp_dist = 1.0
        iters = 0
        while temp_dist > converge_dist:
            #For each point x, select its nearest centroid in i-th centroids set
            cen_rdd1 = rdd.flatMap(lambda each_point: [ ( (indx , j ), each_point[1] ) for (indx,j) in enumerate(self.get_centers(each_point[1], k_points))])

            #Introduce 1 for count
            cen_rdd2 = cen_rdd1.map(lambda ((run, k), pt): ((run, k), np.array([pt, 1])))

            #Add all the distance and add 1's (count)
            cen_rdd3 = cen_rdd2.reduceByKey(lambda x,y: np.sum([x,y], axis=0))

            #Calculate mean distance for each run
            cen_rdd4 = cen_rdd3.map(lambda ((run, k), p):  ((run, k), p[0]/p[1]))

            #Get dictionary of new_points
            new_points = cen_rdd4.collectAsMap()

            temp_dist = np.max([
                    np.sum([np.linalg.norm(k_points[idx][j] - new_points[(idx, j)]) for idx,j in new_points.keys()])
                                ])
            for ((idx, j), p) in new_points.items():
                k_points[idx][j] = p
        self.centroids = k_points

    def save_best_model(self, output_path, rdd, centroids):
        cost = self.get_cost(rdd, centroids)
        log2 = np.log2
        print log2(np.max(cost)), log2(np.min(cost)), log2(np.mean(cost))
        best = np.argmin(cost)

        # dump model as pickle
        with open(output_path, 'wb') as f:
            pickle.dump(centroids[best], f)

        return centroids[best]






