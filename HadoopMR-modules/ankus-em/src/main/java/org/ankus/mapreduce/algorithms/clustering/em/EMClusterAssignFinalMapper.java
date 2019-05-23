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

import org.ankus.util.CommonMethods;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EMClusterAssignFinalMapper extends Mapper<Object, Text, NullWritable, Text>{

	String m_delimiter;
	
	int m_indexArr[];	
	int m_nominalIndexArr[];
	int m_exceptionIndexArr[];
	int class_idx = 0;
	
	int m_clusterCnt;
	EMClusterInfoMgr clusters[];

	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException 			
	{		

	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] columns = value.toString().split(m_delimiter);
		int clusterIndex = -1;
		String writeValueStr = "";
		
		/**
		 * cluster index get
		 */
		double probMax = 0;
        double probSum = 0;
		for(int k=0; k<m_clusterCnt; k++)
		{
			double attrProbSum = 1;
			double attrCnt = 0;
			
			/**
			 * TODO: total distance - probability product sum
			 */
			for(int i=0; i<columns.length; i++)
			{
				double probAttr = 0;

                if(!CommonMethods.isContainIndex(m_exceptionIndexArr, i, false))
                {
                    if(CommonMethods.isContainIndex(m_nominalIndexArr, i, false))
                    {
                        attrCnt = attrCnt + 1;
                        probAttr = clusters[k].getAttributeProbability(i, columns[i], Constants.DATATYPE_NOMINAL);
                        attrProbSum *= probAttr;

                        // write only used attributes
                        if(k==0) writeValueStr += columns[i] + m_delimiter;
                    }
                    else if(CommonMethods.isContainIndex(m_indexArr, i, false))
                    {
                        attrCnt = attrCnt + 1;
                        probAttr = clusters[k].getAttributeProbability(i, columns[i], Constants.DATATYPE_NUMERIC);
                        attrProbSum *= probAttr;

                        // write only used attributes
                        if(k==0) writeValueStr += columns[i] + m_delimiter;
                    }
                }
			}
            double prob = attrProbSum;
			if(prob >= probMax)
			{
				probMax = prob;
				clusterIndex = k;
			}
            probSum += prob;
		}
		double point_ = 1000d;		
        probMax = probMax / probSum;
        probMax = Math.round(probMax * point_)/point_;
        if(class_idx != -1)
            writeValueStr += columns[class_idx] + m_delimiter;
        
		context.write(NullWritable.get(), new Text(writeValueStr + clusterIndex + m_delimiter + probMax));
	}
	

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		
		m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
		
		class_idx = conf.getInt(ArgumentsConstants.CLASS_INDEX, -1);
		
		m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		m_nominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));		
		m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
		
		m_clusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));
		
		
		// cluster load and setting
		Path clusterPath = new Path(conf.get(ArgumentsConstants.CLUSTER_PATH, null));
		clusters = EMClusterInfoMgr.loadClusterInfoFile(conf, clusterPath, m_clusterCnt, m_delimiter);
	}

	

}