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
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class EntropyWeightedMapper extends Mapper<Object, Text, Text, Text>{
	@Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    
    }
	@Override
	protected void map(Object key, Text value, Context context)// throws IOException, InterruptedException
	{
		String[] columns = value.toString().split("\t");
		//Candidate value\t{L,R}:Entropy:{L,R}Count:Total Records
		System.out.println(Arrays.toString(columns));
		try
		{
			//Candidate value  - {L,R}:Entropy:{L,R}Count:Total Records
			context.write(new Text(columns[0]) , new Text(columns[1]));		
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}
	}
}
