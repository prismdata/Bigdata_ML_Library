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
import org.apache.hadoop.io.NullWritable;
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
public class Map_LabelLoader extends Mapper<Object, Text, Text, NullWritable>{

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
      
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

        m_classIndex = Integer.parseInt(conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
        String[] columns = value.toString().split(m_delimiter);
        
       context.write(new Text(columns[m_classIndex].trim()), NullWritable.get());
       
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
