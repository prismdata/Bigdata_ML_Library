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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * NB Reducer
 * @desc
 *
 * @version 0.4
 * @date : 2015.05
 * @author Moonie Song
 */
public class NBStatSumReducer extends Reducer<Text, Text, NullWritable, Text>{

    String m_delimiter;
    int[] m_indexArr;
    int[] m_nominalIndexArr;
    String[] ClassList ;
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

        m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        m_nominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1"));
    }

    //	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        String keyStr = key.toString();
        String keyArr[] = keyStr.split("@@");
        int keyIndex = Integer.parseInt(keyArr[1]);

        int classId = Integer.parseInt(keyArr[0]);
        if(ClassList != null)
        keyArr[0] = ClassList[classId];
        
        Iterator<Text> iterator = values.iterator();
        if(CommonMethods.isContainIndex(m_nominalIndexArr, keyIndex, false))
        {
            // if nominal
            HashMap<String, Long> valMap = new HashMap<String, Long>();
            long dataCnt = 0;
            while (iterator.hasNext())
            {
                String tokens[] = iterator.next().toString().split(m_delimiter);
                long cnt = Long.parseLong(tokens[1]);
                dataCnt += cnt;

                if(valMap.containsKey(tokens[0])) valMap.put(tokens[0], valMap.get(tokens[0]) + cnt);
                else valMap.put(tokens[0], cnt);
            }

            Iterator<String> valIter = valMap.keySet().iterator();
            while(valIter.hasNext())
            {
                String valKeyStr = valIter.next();
                String finalWriteStr = keyArr[1] + m_delimiter
                                        + Constants.DATATYPE_NOMINAL + m_delimiter
                                        + valKeyStr + m_delimiter
                                        + valMap.get(valKeyStr) + m_delimiter
                                        + keyArr[0] + m_delimiter
                                        + dataCnt;

                context.write(NullWritable.get(), new Text(finalWriteStr));
            }
        }
        else
        {
            // if not nominal, then numeric
            double sum = 0;
            double sqareSum = 0;
            long dataCnt = 0;

            while (iterator.hasNext())
            {
                String tokens[] = iterator.next().toString().split(m_delimiter);
                sum += Double.parseDouble(tokens[0]);
                sqareSum += Double.parseDouble(tokens[1]);
                dataCnt += Long.parseLong(tokens[2]);
            }

            double avg = sum / (double)dataCnt;
            double stddev = Math.sqrt((sqareSum / (double)dataCnt) - Math.pow(avg, 2));

            String finalWriteStr = keyArr[1] + m_delimiter
                    + Constants.DATATYPE_NUMERIC + m_delimiter
                    + avg + m_delimiter
                    + stddev + m_delimiter
                    + dataCnt + m_delimiter
                    + keyArr[0] + m_delimiter
                    + dataCnt;

            context.write(NullWritable.get(), new Text(finalWriteStr));
        }
    }



    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
