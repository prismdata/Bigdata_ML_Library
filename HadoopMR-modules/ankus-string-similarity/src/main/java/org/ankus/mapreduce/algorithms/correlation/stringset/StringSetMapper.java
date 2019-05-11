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

package org.ankus.mapreduce.algorithms.correlation.stringset;

import java.io.IOException;

import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * StringSetMapper
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Hamming distance 2. Edit distance
 * @version 0.0.1
 * @date : 2013.07.11
 * @author Suhyun Jeon
*/
public class StringSetMapper extends Mapper<LongWritable, Text, Text, TextTwoWritableComparable> {

	private String computeIndex;
    private String keyIndex;
    private String delimiter;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.computeIndex = configuration.get(Constants.COMPUTE_INDEX);
        this.keyIndex = configuration.get(Constants.KEY_INDEX);
        this.delimiter = configuration.get(Constants.DELIMITER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String row = value.toString();
       String[] columns = row.split(delimiter);
       
       StringBuffer keyIndexStringBuffer = new StringBuffer();
       StringBuffer computeIndexStringBuffer = new StringBuffer();
       
       for(int i=0; i<columns.length; i++){
    	   String column = columns[i];
    	   if(i == Integer.parseInt(keyIndex)){    
    		   keyIndexStringBuffer.append(column);
    	   }else if(i == Integer.parseInt(computeIndex)){
    		   computeIndexStringBuffer.append(column);
    	   }else{
    		   continue;
    	   }
	   }
       
       for(int k=1; k<columns.length; k++){
        	value.set(columns[k]);
           TextTwoWritableComparable textTwoWritableComparable = new TextTwoWritableComparable(keyIndexStringBuffer.toString(), computeIndexStringBuffer.toString());
        	context.write(new Text("item-" + k), textTwoWritableComparable);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}