import yaml
import numpy as np
import pickle
import os

import findspark
findspark.init()

from pyspark import SparkContext
import numpy as np
import pickle
import sys
import csv

import ankus4pass_pyspark.kmeans_pp_cluster as kmeans_pp_cluster

if __name__ == "__main__":
    bath_path = os.path.dirname(os.path.abspath(__file__))
    print bath_path
    yaml_file = open(bath_path + '/etc/spark_config_local.yaml', 'r')

    spark_yaml = yaml.load(yaml_file)
    install_path = spark_yaml["spark_env"]
    SPARK_LOCAL_IP = spark_yaml["SPARK_LOCAL_IP"]
    PYSPARK_DRIVER_PYTHON = spark_yaml["PYSPARK_DRIVER_PYTHON"]
    PYSPARK_DRIVER_PYTHON_OPTS = spark_yaml["PYSPARK_DRIVER_PYTHON_OPTS"]

    os.environ["SPARK_HOME"] = install_path
    os.environ["SPARK_LOCAL_IP"] = SPARK_LOCAL_IP
    os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_DRIVER_PYTHON
    os.environ["PYSPARK_DRIVER_PYTHON_OPTS"] = PYSPARK_DRIVER_PYTHON_OPTS

    bath_path = os.path.dirname(os.path.abspath(__file__))
    model_path = '../iris.pkl'
    input_path = bath_path + '/DataSet/iris.csv'
    if len(sys.argv) == 2:
        model_path = sys.argv[1]
        input_path = sys.argv[2]
    else:
        print 'Arguments is not valid'
        print 'Run with default argument'
        print 'Model Path ' , model_path
        print 'Input Path' , input_path

    #model pkl load
    with open(model_path, 'rb') as f:
        centroid = pickle.load(f)

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

    sc = SparkContext(appName="pyspark_kmeans_test")
    rdd = sc.parallelize(input_raw_data_list)  # 'str type object name, numpy vector'

    print rdd.take(1)

    cluster = kmeans_pp_cluster
    kmeans = cluster.kmeans_pp()

    group2 = rdd.map(
        lambda (in_vector): (in_vector[0], 'cluster:' + str(kmeans.get_closest_centers(in_vector[1], centroid)))).collect()
    for element in group2:
        print element

    sc.stop()
