package org.ankus.mapreduce.algorithms.classification.knn;

import org.apache.hadoop.io.Text;

/**
 * Created with IntelliJ IDEA.
 * User: Wonmoon
 * Date: 15. 3. 31
 * Time: 오후 6:04
 * To change this template use File | Settings | File Templates.
 */
public class DistClassInfo {
    public String className;
    public double distance;
    public String valueText;

    public DistClassInfo(String name, double dist, String value)
    {
        this.className = name;
        this.distance = dist;
        this.valueText = value;
    }

    public DistClassInfo(String name, double dist)
    {
        this.className = name;
        this.distance = dist;
        this.valueText = null;
    }
}
