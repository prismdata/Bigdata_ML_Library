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

//import org.ankus.mapreduce.algorithms.classification.knn.DistClassInfo;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * NB Combiner
 * @desc
 *
 * @version 0.4
 * @date : 2015.05
 * @author Moonie Song
 */
public class NBStatSumCombiner extends Reducer<Text, Text, Text, Text>{

    String m_delimiter;
    int[] m_indexArr;
    int[] m_nominalIndexArr;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

        m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        m_nominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1"));
    }

//	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
        String keyStr = key.toString();
        int index = Integer.parseInt(keyStr.substring(keyStr.indexOf("@@") + 2));        
        Iterator<Text> iterator = values.iterator();
        if(CommonMethods.isContainIndex(m_nominalIndexArr, index, false))
        {
            // if nominal
            HashMap<String, Long> valMap = new HashMap<String, Long>();
            long dataCnt = 0;
            while (iterator.hasNext())
            {
                String val = iterator.next().toString();	
                if(valMap.containsKey(val)) valMap.put(val, valMap.get(val) + 1);
                else valMap.put(val, 1L);
            }

            Iterator<String> valIter = valMap.keySet().iterator();
            while(valIter.hasNext())
            {
                String valStr = valIter.next();
                context.write(key, new Text(valStr + m_delimiter + valMap.get(valStr)));
            }
        }
        else if(CommonMethods.isContainIndex(m_indexArr, index, false))
        {
            // if not nominal, then numeric
            double sum = 0;
            double sqareSum = 0;
            long dataCnt = 0;

            while (iterator.hasNext())
            {
            	String strVal = iterator.next().toString();
            	if(strVal.indexOf(m_delimiter) < 0)
            	{
					double val = Double.parseDouble(strVal);
					sum += val;
					sqareSum += Math.pow(val, 2);
					dataCnt++;
            	}
            }

            String writeStr = sum + m_delimiter + sqareSum + m_delimiter + dataCnt;

            context.write(key, new Text(writeStr));
        }
	}



    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
