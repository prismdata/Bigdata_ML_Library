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

import org.ankus.util.CommonMethods;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 데이터에 군집을 할당하는 클래스.
 * @version 0.0.1
 * @date : 2013.08.22
 * @author Moonie
 */
public class KMeansClusterAssignFinalMapper extends Mapper<Object, Text, NullWritable, Text>{

	private String mDelimiter;          // delimiter for attribute separation
	
	private int mIndexArr[];            // index array used as clustering feature
	private int mNominalIndexArr[];     // index array of nominal attributes used as clustering features
	private int mExceptionIndexArr[];   // index array do not used as clustering features
	
	private int mClusterCnt;            // cluster count
	private KMeansClusterInfoMgr mClusters[];       // clusters
	private int class_idx = 0;
	
	/**
    * 입력 데이터에 대해 클러스터를 할당하기 위해 기본 정보를 읽어온다.
    * @param Context context : 하둡 환경 설정 변수
    * @return void
    * @author Moonie
    */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		
		mDelimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
		
//		class_idx = conf.getInt(ArgumentsConstants.CLASS_INDEX, -1);
		
		mIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		mNominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		mExceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
		
		mClusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));
		
		// cluster load and setting
		Path clusterPath = new Path(conf.get(ArgumentsConstants.CLUSTER_PATH, null));
		mClusters = KMeansClusterInfoMgr.loadClusterInfoFile(conf, clusterPath, mClusterCnt, mDelimiter);
	}
	

	/**
	 * 마지막 저장된 클러스터 중심을 기반으로 분산 입력 파일에 클러스터를 할당한다.
	 * @param Object key : 데이터 오프셋
	 * @param Text value : 입력 값
	 * @param Context context : 하둡 환경 변수.
	 * @author Moonie
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] columns = value.toString().split(mDelimiter);
		int clusterIndex = -1;
		
		String writeValueStr = "";
		/**
		 * cluster index get
		 */
		double distMin = 99999999;		
		for(int k=0; k< mClusterCnt; k++)
		{
			double attrDistanceSum = 0;
			double attrCnt = 0;
			for(int i=0; i<columns.length; i++)
			{
				double distAttr = 0;
                if(!CommonMethods.isContainIndex(mExceptionIndexArr, i, false))
                {
                    if(CommonMethods.isContainIndex(mNominalIndexArr, i, false))
                    {
                        attrCnt++;
                        distAttr = mClusters[k].getAttributeDistance(i, columns[i], Constants.DATATYPE_NOMINAL);
                        attrDistanceSum += Math.pow(distAttr, 2);
                        if(k==0) writeValueStr += columns[i] + mDelimiter;
                    }
                    else if(CommonMethods.isContainIndex(mIndexArr, i, false))
                    {
                        attrCnt++;
                        distAttr = mClusters[k].getAttributeDistance(i, columns[i], Constants.DATATYPE_NUMERIC);
                        attrDistanceSum += Math.pow(distAttr, 2);
                        if(k==0) writeValueStr += columns[i] + mDelimiter;
                    }
                }
			}
			double dist = Math.sqrt(attrDistanceSum);
			if(dist < distMin)
			{
				distMin = dist;
				clusterIndex = k;
			}
		}
		/*
		 * whitepoo
		 * 2018.02.13
		 * 군집에서는 비 사용 코드로 삭제함.
		 */
//		 if(class_idx != -1)
//		        writeValueStr += columns[class_idx] + mDelimiter;
		 double point_ = Math.pow(10,  3);
		distMin = Math.round(distMin * point_) /point_;
		context.write(NullWritable.get(), new Text(writeValueStr + clusterIndex + mDelimiter + distMin));
	}

}
