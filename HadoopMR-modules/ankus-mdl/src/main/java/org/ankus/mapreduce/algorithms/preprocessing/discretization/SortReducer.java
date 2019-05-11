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

package org.ankus.mapreduce.algorithms.preprocessing.discretization;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SortReducer  extends Reducer<DoubleWritable, Text, NullWritable, Text>{

	double max = Double.MIN_VALUE;
	double new_start = 0.0;
	double new_end = 0.0;
	public static enum  LineCounter { LineCount};
	@Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
		Configuration conf = context.getConfiguration();
		new_start = conf.getDouble("new_start", Double.MAX_VALUE);
		new_end  = conf.getDouble("new_end", Double.MIN_VALUE);
    }
	
	protected void reduce(DoubleWritable Attribute, Iterable<Text> Atr_Info, Context context) throws IOException, InterruptedException
	{
		//System.out.println(Attribute.toString());		
		max = Math.max(Attribute.get(), max);			
		Iterator<Text> iterator = Atr_Info.iterator();		 
		while (iterator.hasNext())
		{
		    String tokens = iterator.next().toString();
		    context.write(NullWritable.get(), new Text(tokens)); 
		}
	
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		conf.setDouble("new_max", max);
		
		context.getCounter(LineCounter.LineCount).setDisplayName("LineCnt");//set display name as the max value #it's a trick
		long dbl2longMax = (long)(max * 10000);
		context.getCounter(LineCounter.LineCount).setValue(dbl2longMax);
		System.out.println("Line Max : "+ max);
				
	}
}

