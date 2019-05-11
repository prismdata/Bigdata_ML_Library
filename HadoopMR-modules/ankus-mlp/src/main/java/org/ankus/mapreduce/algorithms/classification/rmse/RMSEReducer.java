/*
 * Copyright (C) 2011 ankus (http://www.openankus.org).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.ankus.mapreduce.algorithms.classification.rmse;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * RMSE Reducer
 * @desc
 *
 * @version 0.1
 * @date : 2016.08.08
 * @author Randol Song
 */
public class RMSEReducer extends Reducer<IntWritable, Text, NullWritable, Text>{

    String m_delimiter;
    int m_classIndex;
    Double max;
    Double min;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER);
        max = Double.parseDouble(conf.get(ArgumentsConstants.CLASS_MAX));
        min = Double.parseDouble(conf.get(ArgumentsConstants.CLASS_MIN));
        m_classIndex = Integer.parseInt(conf.get(ArgumentsConstants.CLASS_INDEX));
//        System.out.println("MAX\t"+conf.get(ArgumentsConstants.CLASS_MAX));
    }

//	@Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
    	Iterator<Text> iterator = values.iterator();
    	String[] gabList;
        
        double original = 0;
        double predicted = 0;
        double rmseSum = 0;
        double maeSum = 0;
        double oriSum = 0;
        double oriAvg = 0;
        int cnt = 0;
        
        ArrayList<Double> oriList = new ArrayList<Double>();
        ArrayList<Double> preList = new ArrayList<Double>();
        
        while (iterator.hasNext()) 
        {	
        	String val = iterator.next().toString();
        	gabList = val.toString().split(m_delimiter);
        	
//        	System.out.println(val);
        	
        	original = Double.parseDouble(gabList[m_classIndex]);
        	predicted = Double.parseDouble(gabList[gabList.length-1]);
        	        	
//        	original = (original+1)/2;
//        	predicted = (predicted+1)/2;

//        	System.out.println(original +"\t"+predicted +"\t"+(max*original + min)+"\t"+(max*predicted + min));
        	
        	original = max*original + min;
        	predicted = max*predicted + min;
        	
        	rmseSum += (original - predicted) * (original - predicted);
        	maeSum += Math.abs(original - predicted);
        	
        	oriSum += original;
        	
        	oriList.add(original);
        	preList.add(predicted);
        	cnt++;
        }
        
        Double rmse = Math.sqrt(rmseSum/cnt);
        Double mae = maeSum/cnt;
        oriAvg = oriSum/cnt;
//        System.out.println(cnt);
//        System.out.println("RMSE:\t"+rmse);
        
        double rrseNumerator = 0.0;
        double rrseDenominator = 0.0;
        
        double rseNumerator = 0.0;
        double rseDenominator = 0.0;
        
        for(int i = 0; i < cnt ; i++){
        	original = oriList.get(i);
        	predicted = preList.get(i);
        	rrseDenominator +=  (original - oriAvg) * (original - oriAvg); 
        	rrseNumerator +=  (predicted - oriAvg) * (predicted - oriAvg); 
        	
        	rseDenominator += Math.abs(original - oriAvg);
        	rseNumerator += Math.abs(predicted - oriAvg);
        }
        Double rrse = Math.sqrt(rrseNumerator/rrseDenominator);
        Double rae = rseDenominator/rseNumerator;
//        System.out.println("RRSE:\t"+rrse);
//    	Relative absolute error                  0.0437 %
//    	Root relative squared error              0.044  %
//    	Total Number of Instances            17379    
        
        StringBuffer retBuf = new StringBuffer();
        retBuf.append("Mean absolute error \t");
        retBuf.append(mae);
        retBuf.append("\r\n");
        
        retBuf.append("Root mean squared error\t");
        retBuf.append(rmse);
        retBuf.append("\r\n");
        
        retBuf.append("Relative absolute error\t");
        retBuf.append(rae);
        retBuf.append("\r\n");
        
        retBuf.append("Root relative squared error\t");
        retBuf.append(rrse);
        retBuf.append("\r\n");
        
        retBuf.append("Total Number of Record\t");
        retBuf.append(cnt);
        retBuf.append("\r\n");
        
//        System.out.println(retBuf.toString());
        context.write(NullWritable.get(), new Text(retBuf.toString()));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
