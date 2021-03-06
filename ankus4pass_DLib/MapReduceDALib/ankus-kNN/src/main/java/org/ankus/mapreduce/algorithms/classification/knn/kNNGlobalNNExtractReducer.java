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
package org.ankus.mapreduce.algorithms.classification.knn;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * knn Reducer
 * @desc
 *
 * @version 0.4
 * @date : 2015.04
 * @author Moonie Song
 */
public class kNNGlobalNNExtractReducer extends Reducer<Text, Text, NullWritable, Text>{

    String m_delimiter;
    int m_kCnt;
    boolean m_isDistanceWeight = false;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        m_kCnt = Integer.parseInt(conf.get(ArgumentsConstants.K_CNT));
        if(conf.get(ArgumentsConstants.DISTANCE_WEIGHT).equals("true")) m_isDistanceWeight = true;
    }

    protected void reduce(Text inputVector, Iterable<Text> itrInput_Model, Context context) throws IOException, InterruptedException
    {
        ArrayList<DistClassInfo> globalKNNList = new ArrayList<DistClassInfo>();
        Iterator<Text> iterator = itrInput_Model.iterator();
        while (iterator.hasNext())
        {
            String valStr = iterator.next().toString();
            String[] tokens = valStr.split(m_delimiter);
            DistClassInfo data = new DistClassInfo(tokens[tokens.length-2], Double.parseDouble(tokens[tokens.length-1]), valStr);

            int index = findInsertIndex(globalKNNList, data, m_kCnt);
            if(index == -1) globalKNNList.add(data);
            else if(index >= 0 ) globalKNNList.add(index, data);
        }

        double sum = 1.0;
        if(m_isDistanceWeight)
        {
            for(int i=0; i<globalKNNList.size(); i++)
            {
                if(i >= m_kCnt) break;
                sum += globalKNNList.get(i).distance;
            }
        }

        HashMap<String, Double> finalClassMap = new HashMap<String, Double>();
        for(int i=0; i<globalKNNList.size(); i++)
        {
            if(i >= m_kCnt) break;
            DistClassInfo data = globalKNNList.get(i);
            double dist = data.distance / sum;

            if(finalClassMap.containsKey(data.className)) dist += finalClassMap.get(data.className);
            finalClassMap.put(data.className, dist);
        }

        DistClassInfo finalClass = new DistClassInfo("NONE", -1.0);
        Set<String> classSet = finalClassMap.keySet();
        for(String classStr: classSet)
        {
            double curDistance = finalClassMap.get(classStr);
            if((finalClass.distance < 0) || (curDistance < finalClass.distance))
            {
                finalClass.className = classStr;
                finalClass.distance = curDistance;
            }
        }

        String writeStr = globalKNNList.get(0).valueText;
        writeStr = writeStr.substring(0, writeStr.lastIndexOf(m_delimiter));    // value delete
        writeStr = writeStr.substring(0, writeStr.lastIndexOf(m_delimiter));    // class delete
        writeStr = writeStr.substring(0, writeStr.lastIndexOf(m_delimiter));    // match key-id delete
        context.write(NullWritable.get(), new Text(writeStr + m_delimiter + finalClass.className));
    }

    public static int findInsertIndex(ArrayList<DistClassInfo> list, DistClassInfo data, int kCnt)
    {
        int size = list.size();
        int retVal = -9;

        if(size == 0) retVal = -1;
        else
        {
            for(int i=0; i<size; i++)
            {
                if(i > kCnt) break;
                if(list.get(i).distance > data.distance)
                {
                    retVal = i;
                    break;
                }
            }

            if((retVal == -9) && (size <= kCnt)) retVal = -1;
        }

        return retVal;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
