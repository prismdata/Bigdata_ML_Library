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
import java.util.HashMap;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MLP_NomalizeNominalReducer1 extends Reducer<IntWritable, Text, NullWritable, Text>{
	private String delimiter;
	private int idxArray[];
	private int classIdx;
	private int nominalIdx[];
	private String[] target;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
        idxArray = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.NOMINAL_INDEX), ",");
        classIdx = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.CLASS_INDEX));
//        nominalIdx = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.NOMINAL_INDEX), ",");
        target = context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX).split(",");
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
    	
    	return map;
    }
    
    private int listLenGab(String[] nominalList, String [] numericList){
    	int totalLen = nominalList.length + numericList.length;
    	int gabLen = 0;
    	return gabLen;
    }
    
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		Iterator<Text> iterator = values.iterator();				
		StringBuffer writeValBuf = new StringBuffer();
		
		String valList[] = null;
		HashMap<String, Integer> nMap = new HashMap<String, Integer>();
		HashMap<String, Integer> cMap = new HashMap<String, Integer>();
		
		int cnt = 0;
		
        while (iterator.hasNext()) 
        {
        	String val = iterator.next().toString();
        	valList = val.split(",");
        	int len = valList.length;

        	cnt = 0;
        	for(int i = 0; i < len; i++){
        		if(map.containsKey(i)){
	        		if(i == classIdx && cnt < idxArray.length && i == idxArray[cnt]){
//	        			System.out.println("classIndex ADDing");
	        			cMap.put(valList[i], 1);
	        		} else if( cnt < idxArray.length && i == idxArray[cnt]){
	        			nMap.put(valList[i], 0);
	        			cnt++;
	        		}
        		}
        	}
        }
        
        Object[] keyList = nMap.keySet().toArray();
        int len = keyList.length;
        writeValBuf.append("n,");
        
        for(int i = 0; i < len; i++){
        	writeValBuf.append(keyList[i]);
        	
        	if(i < len - 1)
        		writeValBuf.append(",");
        }
        writeValBuf.append("\r\n");
        
        keyList = cMap.keySet().toArray();
        len = keyList.length;
        writeValBuf.append("c,");
        for(int i = 0; i < len; i++){
        	writeValBuf.append(keyList[i]);
        	
        	if(i < len - 1)
        		writeValBuf.append(",");
        }
        writeValBuf.append("\r\n");

        context.write(NullWritable.get(), new Text(writeValBuf.toString()));
	}

	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
