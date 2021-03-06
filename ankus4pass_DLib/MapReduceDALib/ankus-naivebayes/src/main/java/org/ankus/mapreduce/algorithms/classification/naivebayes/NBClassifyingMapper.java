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
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * ID3FinalClassifyingMapper
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.12
 * @author Moonie Song
 */
public class NBClassifyingMapper extends Mapper<Object, Text, NullWritable, Text>{

    String m_delimiter;

    ArrayList <String> m_classList = new ArrayList<String>();
    int m_classCnt;
    long m_totalTrainDataCnt;

    /**
     * Key of Map Setting for each class
     *  - class -> "-1"
     *  - numeric -> index
     *  - nominal -> index@@value
     */
    HashMap<String, AttrProbabilityInfo> m_attrProbMap[];
    String m_classKeyStringMap = "-1";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

        // TODO: Distributed Cache File: Rule File
        ArrayList<String[]> readList = new ArrayList<String[]>();
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] targetPathArr = DistributedCache.getLocalCacheFiles(conf);
        for(Path p: targetPathArr)
        {
            FSDataInputStream fin = fs.open(p);
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

            String readStr, tokens[];
            while((readStr=br.readLine())!=null)
            {
                if(readStr.charAt(0)=='#') continue;
                tokens = readStr.split(m_delimiter);
                readList.add(tokens);

                if(tokens[0].equals(Constants.ATTR_CLASS)) m_classList.add(tokens[1]);
            }

            br.close();
            fin.close();
        }

        // rule loading - parsing
        m_classCnt = m_classList.size();
        m_attrProbMap = new HashMap[m_classCnt];
        for(int i=0; i<m_classCnt; i++) m_attrProbMap[i] = new HashMap<String, AttrProbabilityInfo>();

        /**
         * HashMap<String, AttrProbabilityInfo>();
         *  - class -> -1
         *  - numeric -> index
         *  - nominal -> index@@value
         */
        for(String str[]: readList)
        {
            int classIndex = -1;
            if(str[0].equals(Constants.ATTR_CLASS))
            {
                classIndex = m_classList.indexOf(str[1]);
                // class info add
                m_totalTrainDataCnt = Long.parseLong(str[str.length-1]);
                AttrProbabilityInfo tmpProb = new AttrProbabilityInfo(Constants.ATTR_CLASS,
                                                                    m_totalTrainDataCnt,
                                                                    str[1],
                                                                    Double.parseDouble(str[str.length-2]));
                m_attrProbMap[classIndex].put(m_classKeyStringMap, tmpProb);
            }
            else
            {
                classIndex = m_classList.indexOf(str[str.length-2]);
                if(str[1].equals(Constants.DATATYPE_NUMERIC))
                {
                    // numeric add
                    AttrProbabilityInfo tmpProb = new AttrProbabilityInfo(Constants.DATATYPE_NUMERIC,
                            Long.parseLong(str[str.length-1]),
                            Double.parseDouble(str[2]),
                            Double.parseDouble(str[3]));
                    m_attrProbMap[classIndex].put(str[0], tmpProb);
                }
                else if(str[1].equals(Constants.DATATYPE_NOMINAL))
                {
                    // nominal add
                    AttrProbabilityInfo tmpProb = new AttrProbabilityInfo(Constants.DATATYPE_NOMINAL,
                            Long.parseLong(str[str.length-1]),
                            str[2],
                            Double.parseDouble(str[str.length-3]));
                    m_attrProbMap[classIndex].put(str[0] + "@@" + str[2], tmpProb);
                }
            }
        }
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
        String[] columns = value.toString().split(m_delimiter);

        /**
         * first: compute prob about each class
         * second:
         */
        double maxProb = -0.1;
        String maxPorbClassStr = null;
        for(int i=0; i<m_classCnt; i++)
        {
            double curProb = m_attrProbMap[i].get(m_classKeyStringMap).getProb(m_classList.get(i));
            for(int k=0; k<columns.length; k++) curProb *= getCurAttrProb(k, columns[k], i, m_totalTrainDataCnt);

            if(curProb > maxProb)
            {
                maxProb = curProb;
                maxPorbClassStr = m_classList.get(i);
            }
        }

        context.write(NullWritable.get(), new Text(value + m_delimiter + maxPorbClassStr));
	}

    private double getCurAttrProb(int columnIndex, String columnValue, int classIndex, long totalDataCnt)
    {
        double retProb = 0.0;

        String mapKeyStr = columnIndex + "";
        if(m_attrProbMap[classIndex].containsKey(mapKeyStr))
        {
            retProb = m_attrProbMap[classIndex].get(mapKeyStr).getProb(Double.parseDouble(columnValue));
        }
        else
        {
            mapKeyStr = columnIndex + "@@" + columnValue;
            if(m_attrProbMap[classIndex].containsKey(mapKeyStr))
            {
                retProb = m_attrProbMap[classIndex].get(mapKeyStr).getProb(columnValue);
            }
            else
            {
                boolean exist = false;
                for(int i=0; i<m_classCnt; i++)
                {
                    if(i==classIndex) continue;
                    if(m_attrProbMap[i].containsKey(mapKeyStr))
                    {
                        exist = true;
                        break;
                    }
                }

                // if there is in any other class, then return 1/totalDataCnt
                // else if there is not in other class, then rerurn 1.0
                if(exist) retProb = 1.0 / (double)totalDataCnt;
                else return 1.0;
            }
        }

        return retProb;
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
