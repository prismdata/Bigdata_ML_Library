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
package org.ankus.mapreduce.algorithms.clustering.em;

import java.io.IOException;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EMClusterUpdateReducer extends Reducer<IntWritable, Text, NullWritable, Text>{

	String m_delimiter;
	
	int m_indexArr[];	
	int m_nominalIndexArr[];
	int m_exceptionIndexArr[];
	
	int m_clusterCnt;
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException 
	{

	}

//	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
	{
		Iterator<Text> iterator = values.iterator();
				
		EMClusterInfoMgr cluster = new EMClusterInfoMgr();
		cluster.setClusterID(key.get());
		int dataCnt = 0;
		while (iterator.hasNext())
		{
			dataCnt++;
			String tokens[] = iterator.next().toString().split(m_delimiter);
			
			for(int i=0; i<tokens.length; i++)
			{
                if(!CommonMethods.isContainIndex(m_exceptionIndexArr, i, false))
                {
                    if(CommonMethods.isContainIndex(m_nominalIndexArr, i, false))
                        cluster.addAttributeValue(i, tokens[i], Constants.DATATYPE_NOMINAL);
//                    else if(CommonMethods.isContainIndex(m_indexArr, i, true))
                    else if(CommonMethods.isContainIndex(m_indexArr, i, false))
                        cluster.addAttributeValue(i, tokens[i], Constants.DATATYPE_NUMERIC);
                }

//                if(CommonMethods.isContainIndex(m_indexArr, i, true)
//						&& !CommonMethods.isContainIndex(m_exceptionIndexArr, i, false))
//				{
//					if(CommonMethods.isContainIndex(m_nominalIndexArr, i, false))
//					{
//						cluster.addAttributeValue(i, tokens[i], Constants.DATATYPE_NOMINAL);
//					}
//					else cluster.addAttributeValue(i, tokens[i], Constants.DATATYPE_NUMERIC);
//				}
			}
		}
		cluster.finalCompute(dataCnt);
		
		String writeStr = cluster.getClusterInfoString(m_delimiter);
        context.write(NullWritable.get(), new Text(writeStr));
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		
		m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
		
		m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		m_nominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
		
		m_clusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));
	}
}
