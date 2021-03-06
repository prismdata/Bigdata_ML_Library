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

package org.ankus.mapreduce.algorithms.clustering.kmeans;

import java.io.IOException;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 초기 클러스터와 입력 데이터의 거리를 계산하여 클러스터를 결정.
 * 결정된 클러스터와 입력 데이터를 Reducer에 전달.
 * @version 0.0.1
 * @date : 2013.08.22
 * @author Moonie
 */
public class KMeansClusterAssignMapper extends Mapper<Object, Text, IntWritable, Text>{

	private String mDelimiter;                   // delimiter for attribute separation
	
	private int mIndexArr[];                     // index array used as clustering feature
	private int mNominalIndexArr[];              // index array of nominal attributes used as clustering features
	private int mExceptionIndexArr[];            // index array do not used as clustering features
	
	private int mClusterCnt;                     // cluster count
	private KMeansClusterInfoMgr mClusters[];    // clusters
	private String m_distOption;
    
    /**
    * Mapper의 실행 환경을 설정.
    * 컬럼 구분자, 수치데이터 인덱스,범주 데이터 인덱스,예외로 제거할 인덱스, 클러스터 갯수, 거리 측정옵션을 설정.
    * @param Context context : 하둡 시스템과 MapReduce 사이의 상호 작용 인자
    * @author Wonmoon
    */
    @Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		
		mDelimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
		
		mIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		mNominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		mExceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
		
		mClusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));
        m_distOption = conf.get(ArgumentsConstants.DISTANCE_OPTION);
		
		
		// cluster load and setting
		Path clusterPath = new Path(conf.get(ArgumentsConstants.CLUSTER_PATH, null));
		mClusters = KMeansClusterInfoMgr.loadClusterInfoFile(conf, clusterPath, mClusterCnt, mDelimiter);
	}
    
	
	@Override
	/**
    * 입력 데이터(입력 벡터)에 클러스 정보를 할당
    * @param Object key :데이터 오프셋 
    * @param Text value : \n으로 구분되는 라인 단위 입력 데이터.
    * @param Context context : 하둡 시스템과 MapReduce 사이의 상호 작용 인자
    * @author Wonmoon
    */
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] columns = value.toString().split(mDelimiter);
		int clusterIndex = -1;
		
		double distMin = 99999999;		
		for(int k=0; k< mClusterCnt; k++)
		{
			double attrDistanceSum = 0;
			double attrCnt = 0;			
			//거리 측정 방법: 맨허튼 거리.
            if(m_distOption.equals(Constants.CORR_MANHATTAN))
            {
                for(int i=0; i<columns.length; i++)
                {
                    double distAttr = 0;
                    if(!CommonMethods.isContainIndex(mExceptionIndexArr, i, false))
                    {
                        if(CommonMethods.isContainIndex(mNominalIndexArr, i, false))
                        {
                            attrCnt++;
                            distAttr = mClusters[k].getAttributeDistance(i, columns[i], Constants.DATATYPE_NOMINAL);
                            attrDistanceSum += Math.abs(distAttr);
                        }
                        else if(CommonMethods.isContainIndex(mIndexArr, i, true))
                        {
                            attrCnt++;
                            distAttr = mClusters[k].getAttributeDistance(i, columns[i], Constants.DATATYPE_NUMERIC);
                            attrDistanceSum += Math.abs(distAttr);
                        }
                    }
                }

                double dist = attrDistanceSum;
                if(dist < distMin)
                {
                    distMin = dist;
                    clusterIndex = k;
                }
            }
			//거리 측정 방법:유클리드 거리.
            //범주 데이터: 1 -발생빈도의 2제곱의 합의  제곱근을 이용하여 거리를 산출
            //수치 데이터는  중심값과 입력 값의 차의 2제곱에 대한 제곱근을 이용하여 거리를 산출.
            else
            {
                for(int i=0; i<columns.length; i++)
                {
                    double distAttr = 0;
                    //중심과 데이터의 거리 계산.
                    //거리의 제곱을 합한다.
                    if(!CommonMethods.isContainIndex(mExceptionIndexArr, i, false))
                    {
                        if(CommonMethods.isContainIndex(mNominalIndexArr, i, false))
                        {
                            attrCnt++;
                            distAttr = mClusters[k].getAttributeDistance(i, columns[i], Constants.DATATYPE_NOMINAL);
                            attrDistanceSum += Math.pow(distAttr, 2);
                        }
                       else if(CommonMethods.isContainIndex(mIndexArr, i, false))
                        {
                            attrCnt++;
                            distAttr = mClusters[k].getAttributeDistance(i, columns[i], Constants.DATATYPE_NUMERIC);
                            attrDistanceSum += Math.pow(distAttr, 2);
                        }
                    }
                }
                //유클리드 거리로 산출한다.
                double dist = Math.sqrt(attrDistanceSum);
                if(dist < distMin)
                {
                    distMin = dist;
                    clusterIndex = k;
                }
            }
		}		
		context.write(new IntWritable(clusterIndex), value);
	}
}
