import yaml
import numpy as np
import pickle
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



def get_center(object_point, centers):
    center_idx = 0
    closest = [np.inf] * len(centers)
    for idx in range(len(centers)):
        for j in range(len(centers[0])):
            tmp_distence = np.linalg.norm(object_point - centers[idx][j])
            if tmp_distence < closest[idx]:
                closest[idx] = tmp_distence
                center_idx = j
    return center_idx

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

    bath_path = os.path.dirname(os.path.abspath(__file__))
    model_path = bath_path  + '/iris.pkl'
    input_path = bath_path + '/DataSet/iris.csv'
    if len(sys.argv) == 2:
        model_path = sys.argv[1]
        input_path = sys.argv[2]
    else:
        print 'Arguments is not valid'
        print 'Run with default argument'
        print 'Model Path ' , model_path
        print 'Input Path' , input_path

    with open(model_path, 'rb') as f:
        centroid = pickle.load(f)


    input_raw_data_list = []
    f = open(input_path, 'r')
    rdr = csv.reader(f)
    for line in rdr:
        if len(line) > 0:
            object_name = ''.join(line[4:5])
            input_raw_data_list.append((object_name, np.array(list(map(float, line[0:4])))))
    f.close()

    sc = SparkContext(appName="pyspark_kmeans_test")
    rdd = sc.parallelize(input_raw_data_list)
    print rdd.take(1)
    group2 = rdd.map(
        lambda (in_vector): (in_vector[0], 'cluster:' + str(get_center(in_vector[1], centroid)))).collect()
    for element in group2:
        print element

    sc.stop()
