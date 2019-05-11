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

public class MLP_NomalizeNominalReducer2  extends Reducer<IntWritable, Text, NullWritable, Text>{
	private String delimiter;
	private int idxArray[];
	private double minArray[];
	private double maxArray[];
	private int classIdx;
	private String keyList[];
	private int numericLen;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        delimiter = ",";
        idxArray = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.NOMINAL_INDEX), ",");
        classIdx = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.CLASS_INDEX));
        if( context.getConfiguration().get(ArgumentsConstants.NORMAL_NOMINAL_KEY_LIST) == null){
        	//no Nominal Attr
        	keyList = new String[0];
        } else {
        	keyList = context.getConfiguration().get(ArgumentsConstants.NORMAL_NOMINAL_KEY_LIST).split(",");
        }
    }

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		
		HashMap<Integer, String> kMap = getKMap();
		
		Iterator<Text> iterator = values.iterator();

		String writeVal = new String();
		String valList[] = null;
		
		StringBuffer writeBuf = new StringBuffer();
	
		while (iterator.hasNext()) 
        {
        	writeBuf = new StringBuffer();

        	valList = iterator.next().toString().split(",");
        	int len = valList.length;
        	int cnt = 0;
        	HashMap<String, Integer> tmpMap = new HashMap<String, Integer>();
        	
        	for(int i = 0; i < len ; i++){
        		if(i != idxArray[cnt]){
        			writeBuf.append(valList[i]);
        			writeBuf.append(",");
        			
        		} else if( i != classIdx){
        			tmpMap.put(valList[i], 0);
        			
        			if(cnt < idxArray.length -1)
        				cnt++;
        		}
        	} 
        	if(kMap.size() > 0){
	        	Object keyList[] = kMap.keySet().toArray();
	        	int keyLen = keyList.length;
	        	for(int i = 0; i < keyLen ; i++){
	        		if(tmpMap.containsKey(kMap.get(i))){
	        			writeBuf.append("1");
	        		} else {
	        			writeBuf.append("0");
	        		}
	        		writeBuf.append(",");
	        	}
        	}
        	writeBuf.append(valList[classIdx]);
        	context.write(NullWritable.get(), new Text(writeBuf.toString()));
        }
	}

	private boolean isCanBeDouble(String val){
		try {
			Double.parseDouble(val);
			return true;
		} catch(NumberFormatException e){
			return false;
		}
		
	}
	
	private HashMap<Integer, String> getKMap(){
		int keyLen = keyList.length;		
		HashMap<Integer, String> kMap = new HashMap<Integer, String>();
		
		for(int i = 0; i < keyLen ;i++){
			kMap.put(i,keyList[i]);
		}
		
		
		return kMap;
	}
	
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
