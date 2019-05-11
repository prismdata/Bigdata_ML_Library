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

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MLP_TestMapper  extends Mapper<Object, Text, IntWritable, Text>{
	private String delimiter;
    // attribute index array for stat computation
	private int indexArray[];
    // attribute index array for do not computation
	private int exceptionIndexArr[];
	private int classIdx;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // TODO
//        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, ",");
        delimiter = ",";
        indexArray = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX,  "-1"));
        exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        classIdx = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.CLASS_INDEX));
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		context.write(new IntWritable(), new Text(value));
	}
	

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
