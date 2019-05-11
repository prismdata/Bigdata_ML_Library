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

package org.ankus.mapreduce.algorithms.classification.MultilayerPerceptron;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MLP_NomalizeNumericReducer2 extends Reducer<IntWritable, Text, NullWritable, Text>{
	private String delimiter;
	private int idxArray[];
	private double minArray[];
	private double maxArray[];
	private int classIdx;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        delimiter = ",";
        idxArray = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.NUMERIC_INDEX));
        minArray = CommonMethods.convertIndexStr2DoubleArr(context.getConfiguration().get(ArgumentsConstants.MIN_LIST));
        maxArray = CommonMethods.convertIndexStr2DoubleArr(context.getConfiguration().get(ArgumentsConstants.MAX_LIST));
        classIdx = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.CLASS_INDEX));
        String[] target = context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX).split(",");
        this.map = getTargetMap(target);
//       System.out.println("target\t============="+context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX));
    }
    private HashMap<Integer, String> map = new HashMap<Integer, String>();
    
    private HashMap<Integer, String> getTargetMap(String[] list){
    	HashMap<Integer, String> map = new HashMap<Integer, String>();
    	
    	int len = list.length;
    	
    	for(int i = 0; i < len ; i++){
    		map.put(Integer.parseInt(list[i]), list[i]);
    	}
    	System.out.println("Size:\t"+map.size());
    	return map;
    }

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		Iterator<Text> iterator = values.iterator();

		int cnt = 0;
		double maxData = 0;
		double minData = 0;
		double nomValue = 0;
		int len = idxArray.length;
						
        while (iterator.hasNext()) 
        {
        	String val = iterator.next().toString();
        	String[] valList = val.split(",");
        	cnt = 0;
        	StringBuffer writeValBuf = new StringBuffer();
        	for(int i = 0; i < valList.length; i++) {
//        		if(map.containsKey(i)){
//        			System.out.print(i);
	        		if(cnt < len && i == idxArray[cnt]){
	        			double value = Double.parseDouble(valList[i]);
	        			if(classIdx==cnt){
	        				nomValue = (value - minArray[cnt])/(maxArray[cnt]-minArray[cnt]);
	        			}
	        			else{
	        				nomValue = 2*(value - minArray[cnt])/(maxArray[cnt]-minArray[cnt]) - 1;
	        			}
	    	        	writeValBuf.append(nomValue);
	    	        	cnt++;
	        		}
	        		else {
	    	        	writeValBuf.append(valList[i]);
	        		}
	        		if(i < valList.length -1)
	        			writeValBuf.append(delimiter);
//        		} else{
//        			writeValBuf.append(valList[i]);
//        			if(i < valList.length -1)
//	        			writeValBuf.append(delimiter);
//        			
//        		}
//        		System.out.println("writeBuf:+\t"+writeValBuf.toString());
        	}
        	writeValBuf.append("\r\n");
        	context.write(NullWritable.get(), new Text(writeValBuf.toString()));
        }
        
	}
	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
