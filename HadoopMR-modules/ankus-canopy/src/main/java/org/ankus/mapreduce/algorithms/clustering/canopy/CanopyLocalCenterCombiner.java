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

package org.ankus.mapreduce.algorithms.clustering.canopy;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;


public class CanopyLocalCenterCombiner extends Reducer<IntWritable, Text, IntWritable, Text>{

	String mDelimiter;              // delimiter for attribute separation
	
	int mIndexArr[];                // index array used as clustering feature
	int mNominalIndexArr[];         // index array of nominal attributes used as clustering features
	int mExceptionIndexArr[];       // index array do not used as clustering features
	
	double mT1;
    double mT2;
    String mDistOption;
    double mNominalDistBase = 1;

    IntWritable baseKey = new IntWritable(1);

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        mDelimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

        mIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
        mNominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
        mExceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));

        mT1 = Double.parseDouble(conf.get(ArgumentsConstants.CANOPY_T1));
        mT2 = Double.parseDouble(conf.get(ArgumentsConstants.CANOPY_T2));

        mDistOption = conf.get(ArgumentsConstants.DISTANCE_OPTION);
    }

//	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
	{
		Iterator<Text> iterator = values.iterator();
        ArrayList<String> dataList = new ArrayList<String>();

        
        	while (iterator.hasNext()) dataList.add(iterator.next().toString());

	        while(!dataList.isEmpty())
	        {
	            String d = dataList.get(0);
	            
	            context.write(baseKey, new Text(d));
	            dataList.remove(0);
	            String d1[] = d.split(mDelimiter);
	
	            ArrayList<Integer> removedIndex = new ArrayList<Integer>();
	            for(int i=0; i<dataList.size(); i++)
	            {
	            	String d_i  = "";
	            	try
	            	{
		                d_i = dataList.get(i);
		               
		               	double dist = CommonMethods.getDistance(d1, d_i.split(mDelimiter), mDistOption, mIndexArr, mNominalIndexArr, mExceptionIndexArr, -1, mNominalDistBase);
			                //if(dist < mT1) canopy.elements.add(d_i);
			            if(dist < mT2) removedIndex.add(0, i);	
		               
	            	}
	            	catch(Exception e)
	            	{
	            		e.printStackTrace();
	            		System.out.println(e.toString());
	            	}
	            }
	
	            for(int i: removedIndex) dataList.remove(i);        
        
	}
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {

    }
}
