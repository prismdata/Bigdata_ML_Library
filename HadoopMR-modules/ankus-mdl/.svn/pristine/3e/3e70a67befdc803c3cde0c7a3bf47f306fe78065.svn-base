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
import java.util.HashMap;

//import org.ankus.mapreduce.algorithms.classification.rulestructure.RuleNodeBaseInfo;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class EntropyDisSelAttributeMapper extends Mapper<Object, Text, Text, Text>{
	String m_delimiter;
    String m_ruleCondition;
 
    int m_numericIndexArr[];
    int m_exceptionIndexArr[];
    int m_classIndex;
     
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    	
    }
    
	@Override
	protected void map(Object key, Text value, Context context)// throws IOException, InterruptedException
	{
		String inputValue = value.toString();
		String[] columns = inputValue.split("\t");
		try
		{
			String postFix = columns[1].substring(0, 1);			
			context.write(new Text(columns[0] + postFix) , new Text(columns[1]));		
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}
	}
	
}
