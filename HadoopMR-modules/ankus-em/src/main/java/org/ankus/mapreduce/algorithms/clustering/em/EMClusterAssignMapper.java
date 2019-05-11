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

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EMClusterAssignMapper extends Mapper<Object, Text, IntWritable, Text>{

	String m_delimiter;

	int m_indexArr[];
	int m_nominalIndexArr[];
	int m_exceptionIndexArr[];

	int m_clusterCnt;
	EMClusterInfoMgr clusters[];

    boolean isInitial;


	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{

	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
        int clusterIndex = -1;

        if(isInitial)
        {
            clusterIndex = (int) (Math.random() * m_clusterCnt);
        }
        else
        {
            String[] columns = value.toString().split(m_delimiter);

            /**
             * cluster index get
             */
            double probMax = 0;
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
                        }
                        else if(CommonMethods.isContainIndex(m_indexArr, i, true))
                        {
                            attrCnt = attrCnt + 1;
                            probAttr = clusters[k].getAttributeProbability(i, columns[i], Constants.DATATYPE_NUMERIC);
                            attrProbSum *= probAttr;
                        }
                    }




//                    if(CommonMethods.isContainIndex(m_indexArr, i, true)
//                            && !CommonMethods.isContainIndex(m_exceptionIndexArr, i, false))
//                    {
//                        attrCnt = attrCnt + 1;
//                        if(CommonMethods.isContainIndex(m_nominalIndexArr, i, false))
//                        {
//                            probAttr = clusters[k].getAttributeProbability(i, columns[i], Constants.DATATYPE_NOMINAL);
//                        }
//                        else probAttr = clusters[k].getAttributeProbability(i, columns[i], Constants.DATATYPE_NUMERIC);
//
//    //					attrProbSum += probAttr;
//                        attrProbSum *= probAttr;
//                    }

                }

    //			double prob = attrProbSum / attrCnt;
                double prob = attrProbSum;
                if(prob >= probMax)
                {
                    probMax = prob;
                    clusterIndex = k;
                }
            }
            // probability normalizing
        }


		context.write(new IntWritable(clusterIndex), value);
	}


	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();

		m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

		m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		m_nominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));

		m_clusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));


        if(conf.get("IS_INITIAL", "FALSE").equals("TRUE")) isInitial = true;
        else
        {
            isInitial = false;
            // cluster load and setting
            Path clusterPath = new Path(conf.get(ArgumentsConstants.CLUSTER_PATH, null));
            clusters = EMClusterInfoMgr.loadClusterInfoFile(conf, clusterPath, m_clusterCnt, m_delimiter);
        }
	}

	

}
