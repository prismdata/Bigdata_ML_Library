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
import org.ankus.util.CommonMethods;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * knn Combiner
 * @desc
 *
 * @version 0.4
 * @date : 2015.04
 * @author Moonie Song
 */
public class kNNLocalNNExtractCombiner extends Reducer<Text, Text, Text, Text>{
	private Logger logger = LoggerFactory.getLogger(kNNLocalNNExtractCombiner.class);
    String m_delimiter;
    int m_kCnt;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        m_kCnt = Integer.parseInt(conf.get(ArgumentsConstants.K_CNT));
    }

	protected void reduce(Text inputVector, Iterable<Text> input_Model_Info, Context context) throws IOException, InterruptedException
	{
        ArrayList<DistClassInfo> localKNNList = new ArrayList<DistClassInfo>();
        Iterator<Text> iterator = input_Model_Info.iterator();
        while (iterator.hasNext())
        {
        	String valStr = iterator.next().toString();
            String[] tokens = valStr.split(m_delimiter);
            String Class = tokens[tokens.length-2];
            double distance = Double.parseDouble(tokens[tokens.length-1]);
            
            DistClassInfo data = new DistClassInfo(Class,distance,valStr);
            
            int index = kNNGlobalNNExtractReducer.findInsertIndex(localKNNList, data, m_kCnt);
            if(index == -1) localKNNList.add(data);
            else if(index >= 0 ) localKNNList.add(index, data);
        }

        for(int i=0; i<localKNNList.size(); i++)
        {
            if(i >= m_kCnt) break;
            String strInputVector = inputVector.toString();
            String[] AryInputVector = strInputVector.split(":");
            Text key_text = new Text(strInputVector);
            context.write(key_text, new Text(localKNNList.get(i).valueText));
        }
	}



    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
