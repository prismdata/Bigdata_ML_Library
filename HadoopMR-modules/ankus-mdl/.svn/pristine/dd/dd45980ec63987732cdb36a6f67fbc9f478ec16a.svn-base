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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class EntropyCountClassReducer  extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
		
    }
	protected void reduce(Text Attribute, Iterable<IntWritable> Class_Appears, Context context) throws IOException, InterruptedException
	{
		int sum = 0;
		for(IntWritable val: Class_Appears)
		{
			sum += val.get();
		}
		
		context.write(Attribute, new IntWritable(sum));
	}
	public void cleanup(Context context) throws IOException, InterruptedException{
		
	}
}
