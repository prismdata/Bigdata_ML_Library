/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ankus.mapreduce.algorithms.classification.naivebayes;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * NB Mapper
 * @desc
 *
 * @version 0.4
 * @date : 2015.05
 * @author Moonie Song
 */
public class NBSplitMapper extends Mapper<Object, Text, Text, Text>{

    String m_delimiter;
    int[] m_indexArr;
    int[] m_nominalIndexArr;
    int[] m_exceptionIndexArr;
    String[] ClassList ;
    int m_classIndex;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        String strClassList = conf.get(ArgumentsConstants.CLASS_LIST, "-1");
        if(strClassList.equals("-1") == false)
        {
        	ClassList = CommonMethods.convertIndexStr2StringArr(strClassList);
        }
        
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

        m_classIndex = Integer.parseInt(conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
        m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        m_nominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
        m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
        String[] columns = value.toString().split(m_delimiter);
        
        for(int i=0; i<columns.length; i++)
        {
        	if(ClassList != null)
	        {
	        	for(int ci = 0; ci < ClassList.length; ci++)
	        	{
	        		columns[m_classIndex] = columns[m_classIndex].trim();
	        		if(ClassList[ci].equals(columns[m_classIndex]))
					{
	        			columns[m_classIndex] = ci+"";
					}
	        	}
        	}
        	columns[i] = columns[i].trim();
            if(isValidIndex(i)) context.write(new Text(columns[m_classIndex] + "@@" + i), new Text(columns[i]));
        }

        Counter counter = context.getCounter("NAIVEBAYES","MAPCOUNT");
        counter.increment(1);
	}

    private boolean isValidIndex(int i)
    {
        if((i!=m_classIndex)
                && (!CommonMethods.isContainIndex(m_exceptionIndexArr, i, false)))
        {
            if(CommonMethods.isContainIndex(m_indexArr, i, true)
                    || CommonMethods.isContainIndex(m_nominalIndexArr, i, false)) return true;
        }

        return false;
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
