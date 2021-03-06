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
public class Reducer_LabelWriter extends Reducer<Text, NullWritable, Text, NullWritable>{

    String m_delimiter;
    int[] m_indexArr;
    int[] m_nominalIndexArr;
    String[] ClassList ;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        
    }

    //	@Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
    {
    	System.out.println(key.toString());
        context.write(key, NullWritable.get());
    }



    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
