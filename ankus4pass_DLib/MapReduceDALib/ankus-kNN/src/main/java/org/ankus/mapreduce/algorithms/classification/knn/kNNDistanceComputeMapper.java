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
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * kNN Mapper
 * @desc
 *
 * @version 0.4
 * @date : 2015.04
 * @author Moonie Song
 */
public class kNNDistanceComputeMapper extends Mapper<Object, Text, Text, Text>{

    String m_delimiter;
    int[] m_indexArr;
    int[] m_nominalIndexArr;
    int[] m_exceptionIndexArr;

    int m_classIndex;
    String m_keyIndexStr;
    String m_distOption;
    double m_nominalDistBase;
    boolean m_isExceptKeyIndex = false;

    ArrayList<String> inputVectorList = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

//        m_keyIndex = Integer.parseInt(conf.get(ArgumentsConstants.KEY_INDEX));
        m_classIndex = Integer.parseInt(conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
        m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        m_nominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
        m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));

        m_nominalDistBase = Double.parseDouble(conf.get(ArgumentsConstants.NOMINAL_DISTANCE_BASE));
        m_distOption = conf.get(ArgumentsConstants.DISTANCE_OPTION);

        if(conf.get(Constants.DUPLICATE_KEY_EXCEPTION,"").equals("true")) m_isExceptKeyIndex = true;

        inputVectorList = new ArrayList<String>();
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] targetPathArr = DistributedCache.getLocalCacheFiles(conf);
        for(Path p: targetPathArr)
        {
            FSDataInputStream fin = fs.open(p);
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

            String readStr;
            while((readStr=br.readLine())!=null) inputVectorList.add(readStr);

            br.close();
            fin.close();
        }
    }
    /*
     * 2017-12-04 whitepoo@onycom.com
     * 데이터 값이 동일한 경우 key 충돌로 인한 데이터 누락 현상이 발생하여 입력 데이터의 인덱스를 데이터화 결합한다.
     */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
        String[] model_Columns = value.toString().split(m_delimiter);
        String model_input_desc = "";
        int input_size = inputVectorList.size();
        /*
         * 2017-12-04 whitepoo@onycom.com
         * 입력 데이터와 인덱스 결합.
         */
        for(int input_idx = 0; input_idx < input_size; input_idx++)
        {
        	String[] testArr = inputVectorList.get(input_idx).split(m_delimiter);

        	model_input_desc = CommonMethods.genKeyStr(testArr) + m_delimiter +
        			testArr[m_classIndex] + m_delimiter +
        			CommonMethods.getDistance(testArr, testArr, m_distOption, m_indexArr, m_nominalIndexArr, m_exceptionIndexArr, m_classIndex, m_nominalDistBase);
        	//Key : inputVector
            //Value : inputVector + input_model distance
            Text emit_key = new Text(input_idx + ":" + inputVectorList.get(input_idx));
            Text emit_value = new Text(inputVectorList.get(input_idx) + m_delimiter + model_input_desc);
            context.write(emit_key,emit_value);            
        }
        /*
         * 2017-12-04 whitepoo@onycom.com
         * 데이터 자체를 키로 사용.
         */
//        String[] columns = value.toString().split(m_delimiter);
//        String writeVal = "";
//        for(String dataStr: m_dataList)
//        {
//            String[] testArr = dataStr.split(m_delimiter);
//            if(m_isExceptKeyIndex && CommonMethods.genKeyStr(columns).equals(CommonMethods.genKeyStr(testArr))) 
//        	{
//            	continue;
//        	}
//            writeVal = CommonMethods.genKeyStr(columns) + m_delimiter +
//                        columns[m_classIndex] + m_delimiter +
//                        CommonMethods.getDistance(columns, testArr, m_distOption, m_indexArr, m_nominalIndexArr, m_exceptionIndexArr, m_classIndex, m_nominalDistBase);
//            context.write(new Text(CommonMethods.genKeyStr(testArr)), new Text(dataStr + m_delimiter + writeVal));
//        }
        
       
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        if(inputVectorList!=null) inputVectorList.clear();
        System.gc();
    }
}
