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

package org.ankus.mapreduce.algorithms.classification.C45;

import org.ankus.mapreduce.algorithms.classification.C45.*;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * C45ComputeEntropyReducer
 * @desc
 *
 * @version 0.1
 * @date : 2016.04.04
 * @author SHINHONGJOONG
 */
public class C45ComputeEntropyReducer extends Reducer<Text, Text, NullWritable, Text>{
	private org.slf4j.Logger logger = LoggerFactory.getLogger(C45ComputeEntropyReducer.class);
	int mIndexArr[];                     // index array used as clustering feature
	int mNominalIndexArr[];              // index array of nominal attributes used as clustering features
	int mExceptionIndexArr[];            // index array do not used as clustering features
	int m_classIndex;
    String m_delimiter;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        String numeric_idx = "",norminal_idx="";
        
        numeric_idx = conf.get(ArgumentsConstants.TARGET_INDEX,  "-1");
        norminal_idx = conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1");
        
		mIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		mNominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		mExceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
		String class_idx = conf.get(ArgumentsConstants.CLASS_INDEX, "-1");
        if(CommonMethods.convertIndexStr2IntArr(numeric_idx).length > 1)
        {
        	if(numeric_idx.contains(","+class_idx))
	        {
	        	numeric_idx = numeric_idx.replace(","+class_idx, "");
	        }
	        if(numeric_idx.contains(class_idx + ","))
	        {
	        	numeric_idx = numeric_idx.replace(class_idx + ",", "");
	        }
        }
        else
        {
        	if(numeric_idx.contains(class_idx))
	        {
	        	numeric_idx = numeric_idx.replace(class_idx, "-1");
	        }
        }
        if(CommonMethods.convertIndexStr2IntArr(norminal_idx).length > 1)
        {
	        if(norminal_idx.contains(","+class_idx))
	        {
	        	norminal_idx = norminal_idx.replace(","+class_idx, "");
	        }
	        if(norminal_idx.contains(class_idx + ","))
	        {
	        	norminal_idx = norminal_idx.replace(class_idx + ",", "");
	        }
        }
        else
        {
        	if(norminal_idx.contains(class_idx))
	        {
	        	norminal_idx = norminal_idx.replace(class_idx, "-1");
	        }
        }
        
        mNominalIndexArr = CommonMethods.convertIndexStr2IntArr(norminal_idx);//GA를 위한 인자 변경.
        mIndexArr = CommonMethods.convertIndexStr2IntArr(numeric_idx);//GA를 위한 인자 변경.
        m_classIndex = Integer.parseInt(conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
        
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
    }

	@SuppressWarnings("unchecked")
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		
			int rec_count = 0;
	        Iterator<Text> iterator = values.iterator();
	        HashMap<String, Integer> attrCount = new HashMap<String, Integer>();
	        
	        HashMap<String, HashMap<String, Integer>> attrClassList = new HashMap<String, HashMap<String, Integer>>();
	        ArrayList<String> valueList = new ArrayList<String>();
	        while (iterator.hasNext())
	        {
	        	rec_count++;
	            String tokens[] = iterator.next().toString().split(m_delimiter);
	            HashMap classMap = null;
	            if(attrClassList.containsKey(tokens[0]))//Contains Attribute
	            {
					classMap = attrClassList.get(tokens[0]);
	                if(classMap.containsKey(tokens[1])) //Contains Class
                    {
	                	classMap.put(tokens[1], (Integer)classMap.get(tokens[1])+1);
                    }
	                else
                	{
	                	classMap.put(tokens[1], 1);
                	}
	            }
	            else
	            {
	                classMap = new HashMap<String, Integer>();
	                //Map<Class, 1>
	                classMap.put(tokens[1], 1);
	                valueList.add(tokens[0]);
	            }
	            //Map<Attribute Value, <Class, 1>>
                attrClassList.put(tokens[0], classMap);
                //For missing value
                if(attrCount.containsKey(tokens[0]) == true)
                {
                	int count = attrCount.get(tokens[0]);
                	attrCount.put(tokens[0], count+1);
                }
                else
                {
                	attrCount.put(tokens[0], 1);
                }
	        }
	        String missing_attribute = "";
	        if(attrCount.containsKey("?") == true)
	        {
	        	//Get maximum attribute value except "?"
	        	int maxAppears = Integer.MAX_VALUE * -1;
	            for( Entry<String, Integer> elem : attrCount.entrySet() )
	            {
	            	if(elem.getKey().equals("?") == false)
	            	{
	            		if( maxAppears <elem.getValue())
	            		{
	            			maxAppears = elem.getValue();
	            			missing_attribute = elem.getKey();
	            		}
	            	}
	            }
	        }
	        //Replace missing value
	        HashMap<String, Integer> missing_class_dist =  attrClassList.get("?");
	        if(missing_class_dist != null)
	        {
	        	attrClassList.put(missing_attribute, missing_class_dist);
	        }
	        Reducer_EntropyInfo e = new Reducer_EntropyInfo();
	        Configuration conf = context.getConfiguration();
	        int ColumnIdx = Integer.parseInt(key.toString()); 
	        //수치 인덱스가 지정되어 있지 않을 경우 false
	        if(CommonMethods.isContainIndex(mIndexArr, ColumnIdx , false) == true)
	        {
	        	e.setValueList(valueList);
	        	String attribute_idx = key.toString();
	        	e.computeGainRatio(conf, attribute_idx, attrClassList);
	        	String str_Distribution = e.putAttributeDist();
	        	if(str_Distribution != null)
	        	{
		        	context.write(NullWritable.get(), new Text(key.toString() + m_delimiter + str_Distribution));
	        	}
	        	else
	        	{
	        		logger.error("Distribution Is null");
	        	}
	        }
	        else if(CommonMethods.isContainIndex(mNominalIndexArr, ColumnIdx , false) == true)
	        {
	        	e.setValueList(valueList);
	        	
	        	String attribute_idx = key.toString();
	        	e.computeGainRatio(conf, attribute_idx, attrClassList);
	        	String str_Distribution = e.putAttributeDist();
	        	if(str_Distribution != null)
	        	{
		        	context.write(NullWritable.get(), new Text(key.toString() + m_delimiter + str_Distribution));
	        	}  	      
	        	else
	        	{
	        		logger.error("Distribution Is null");
	        	}
	        }  
        
	}
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
		//System.out.println("redecer out");
    }
}

	        