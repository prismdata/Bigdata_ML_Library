#-*- coding: utf-8 -*-

import yaml
import os

from pyspark import SparkContext
import numpy as np
import sys
import csv

import findspark


if __name__ == "__main__":
    bath_path = os.path.dirname(os.path.abspath(__file__))
    print bath_path
    yaml_file = open(bath_path + '/../etc/spark_config_local.yaml', 'r')

    spark_yaml = yaml.load(yaml_file)
    install_path = spark_yaml["spark_env"]
    SPARK_LOCAL_IP = spark_yaml["SPARK_LOCAL_IP"]
    PYSPARK_DRIVER_PYTHON = spark_yaml["PYSPARK_DRIVER_PYTHON"]
    PYSPARK_DRIVER_PYTHON_OPTS = spark_yaml["PYSPARK_DRIVER_PYTHON_OPTS"]

    os.environ["SPARK_HOME"] = install_path
    os.environ["SPARK_LOCAL_IP"] = SPARK_LOCAL_IP
    os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_DRIVER_PYTHON
    os.environ["PYSPARK_DRIVER_PYTHON_OPTS"] = PYSPARK_DRIVER_PYTHON_OPTS
    findspark.init()
    K = 10
    numbers_of_parallel_kmeans = 20
    converge_dist = 0.03
    bath_path = os.path.dirname(os.path.abspath(__file__))
    input_path = bath_path + '/../DataSet/iris.csv'
    output_path = 'iris.pkl'
    if len(sys.argv) == 4:
        K = int(sys.argv[1])
        numbers_of_parallel_kmeans = int(sys.argv[2])  # '병렬로 실핼할 kmeans갯수'
        converge_dist = float(sys.argv[3])
        input_path = bath_path + sys.argv[4]
        output_path = sys.argv[5]

    else:
        print 'Arguments is not valid'
        print 'Run with default argument'
        print 'K', K, 'Parallel Kmeans', numbers_of_parallel_kmeans, ' Converge Dist', converge_dist
        print 'source data', input_path
    seed = 1000

    vector_list = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width']
    object_name = ['class']

    input_raw_data_list = []

    with open(input_path, "r") as f:
        mycsv = csv.DictReader(f)
        for row in mycsv:
            vector_values = []
            for col in vector_list:
                vector_values.extend([row[col]])
            vector_values_np = np.array(list(map(float, vector_values)))
            for col in object_name:
                input_raw_data_list.append((row[col], vector_values_np))

    sc = SparkContext(appName="pyspark_kmeans")
    rdd = sc.parallelize(input_raw_data_list)  # 'str type object name, numpy vector'

    print('K', K)
    print('RUNS', numbers_of_parallel_kmeans)
    np.random.seed(seed)

    kmeans_pp = kmeans_pp_cluster.kmeans_pp()

    kmeans_pp.fit(rdd, K, numbers_of_parallel_kmeans, converge_dist, np.random.randint(1000))

    best_model = kmeans_pp.save_best_model(output_path, rdd, kmeans_pp.centroids)
    group2 = rdd.map(lambda (in_vector): (
    in_vector[0], 'cluster:' + str(kmeans_pp.get_closest_centers(in_vector[1], best_model)))).collect()
    for element in group2:
        print element

    sc.stop()
