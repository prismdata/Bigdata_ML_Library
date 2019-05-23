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

package org.ankus.mapreduce.algorithms.correlation.columnbase;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * ColumnCorrMapper
 * @desc
 * @version
 * @date :
 * @author Moonie
 */
public class ColumnCorrMapper extends Mapper<Object, Text, Text, Text>{

	private String delimiter;
	private int indexArray[];
	private int exceptionIndexArr[];

    private String definedDelimiter = "@@";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
        indexArray = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX,  "-1"));
        exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] columns = value.toString().split(delimiter);
        ArrayList<Integer> targetIndexList = new ArrayList<Integer>();
		
		for(int i=0; i<columns.length; i++)
		{
			if(CommonMethods.isContainIndex(indexArray, i, true) && !CommonMethods.isContainIndex(exceptionIndexArr, i, false))
			{
                targetIndexList.add(i);
			}
		}

        if(targetIndexList.size()>1)
        {
            int size = targetIndexList.size();
            Integer targetIndexArr[] = new Integer[size];
            targetIndexArr = targetIndexList.toArray(targetIndexArr);

            for(int i=0; i<size; i++)
            {
                for(int j=i+1; j<size; j++)
                {
                    context.write(new Text(targetIndexArr[i] + definedDelimiter + targetIndexArr[j]), new Text(columns[i] + definedDelimiter + columns[j]));
                }
            }
        }
	}


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    	System.out.println("map cleanup");
    }
}