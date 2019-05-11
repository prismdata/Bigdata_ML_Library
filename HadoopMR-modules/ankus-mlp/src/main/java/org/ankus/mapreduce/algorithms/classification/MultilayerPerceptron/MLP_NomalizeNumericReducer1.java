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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NumericStats1_1MRReducer
 * @desc reducer class for numeric statistics computation mr job (1-step)
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class MLP_NomalizeNumericReducer1 extends Reducer<IntWritable, Text, NullWritable, Text>{

	private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
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
    	
    	return map;
    }

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		Iterator<Text> iterator = values.iterator();
						
		long cnt = 0;
	
		double maxData = 0;
		double minData = 0;
		
        while (iterator.hasNext()) 
        {
        	double value = Double.parseDouble(iterator.next().toString());
        	//don't convert str2doubles
        	cnt++;
        	
        	if(cnt==1)
			{
				maxData = value;
				minData = value;
			}
			else
			{
				if(maxData < value) maxData = value;
				if(minData > value) minData = value;
			}
        }

        String writeVal = maxData + delimiter +
				minData;
        context.write(NullWritable.get(), new Text(key.toString() + delimiter + writeVal));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
