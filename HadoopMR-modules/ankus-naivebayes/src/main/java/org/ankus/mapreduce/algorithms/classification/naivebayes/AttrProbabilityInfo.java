package org.ankus.mapreduce.algorithms.classification.naivebayes;


import org.ankus.util.Constants;

import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: Wonmoon
 * Date: 15. 5. 27
 * Time: 오후 3:00
 * To change this template use File | Settings | File Templates.
 */
public class AttrProbabilityInfo {

    public String valueDataType = null;
    public long totalDataCnt;

    public double average;
    public double variance;

    public String categoryValue;
    public double freq;

    public AttrProbabilityInfo(String type, long totalCnt, double avg, double stddev)
    {
        this.valueDataType = type;
        this.totalDataCnt = totalCnt;
        this.average = avg;
        this.variance = Math.pow(stddev, 2);
    }

    public AttrProbabilityInfo(String type, long totalCnt, String category, double freq)
    {
        this.valueDataType = type;
        this.totalDataCnt = totalCnt;
        this.categoryValue = category;
        this.freq = freq;
    }

    public double getProb(double val)
    {
        if(variance == 0)
        {
            if(val == average) return 1.0;
            else return 0.0;
        }
        else
        {
            double powVal = -1.0 * Math.pow(val-average, 2) / (2.0 * variance);
            double baseVal = 1.0 / Math.sqrt(2.0 * Math.PI * variance);

            return baseVal * Math.pow(Math.E, powVal);
        }
    }

    public double getProb(String val)
    {
        if(categoryValue.equals(val)) return (double)freq / (double)totalDataCnt;
        else return 0.0;
    }

}
