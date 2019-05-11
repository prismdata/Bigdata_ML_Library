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

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class EntropyEvaluMapper extends Mapper<Object, Text, Text, Text>{
	Double split_point = 0.0;
	//Double entropy = 0.0; 
	String m_delimiter = "";
	int m_indexArr;
	int m_classIndex;
	String nextBin  = "";
	double new_start = 0.0, new_end =0.0;
	boolean leftsubset = false;
	@Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
		Configuration conf = context.getConfiguration();
		
		split_point = conf.getDouble("split_point", -1);		
		//m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
		m_indexArr =  conf.getInt("FilterTarget", -1);
		
		m_classIndex = Integer.parseInt(conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
		
		nextBin = conf.get("nextBin", "F");
		m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
		
		new_start = conf.getDouble("new_start", Double.MAX_VALUE);
		new_end = conf.getDouble("new_end", Double.MIN_VALUE);
		leftsubset = conf.getBoolean("leftsubset", false);
    }
	@Override
	protected void map(Object key, Text value, Context context)// throws IOException, InterruptedException
	{
		String[] columns = value.toString().split(m_delimiter);
		try
		{
			double source = Double.parseDouble(columns[m_indexArr]);
			if(leftsubset == true)
			{
				if((new_start < source) && ( source < new_end))
				{
					if(source <= split_point)
					{
						context.write(new Text("U") , new Text(columns[m_classIndex]));		
					}
					else
					{
						context.write(new Text("D") , new Text(columns[m_classIndex]));		
					}
					context.write(new Text("F") , new Text(columns[m_classIndex]));
				}
			}
			else
			{
				if((new_start <= source) && ( source <= new_end))
				{
					if(source <= split_point)
					{
						context.write(new Text("U") , new Text(columns[m_classIndex]));		
					}
					else
					{
						context.write(new Text("D") , new Text(columns[m_classIndex]));		
					}
					context.write(new Text("F") , new Text(columns[m_classIndex]));
				}
				
			}
			
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}
	}
}
