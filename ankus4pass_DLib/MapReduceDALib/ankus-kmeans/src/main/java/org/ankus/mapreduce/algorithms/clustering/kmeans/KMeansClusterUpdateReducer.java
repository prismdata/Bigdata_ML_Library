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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 결정된 클러스터 번호와 데이터를 받아.클러스터별 데이터의 갯수를 기반으로, 각 클러스터의 중심 정보를 변경한다.
 * @version 0.0.1
 * @date : 2013.08.22
 * @author Moonie
 */
public class KMeansClusterUpdateReducer extends Reducer<IntWritable, Text, NullWritable, Text>{

	private String mDelimiter;              // delimiter for attribute separation
	
	private int mIndexArr[];                // index array used as clustering feature
	private int mNominalIndexArr[];         // index array of nominal attributes used as clustering features
	private int mExceptionIndexArr[];       // index array do not used as clustering features
	private int mClusterCnt;                // cluster count
	/**
	 * 하둡 환경 설정 변수로 부터 변수간 구분자, 사용할 수치 변수 인덱스 리스트, 범주형 변수 인덱스 리스트, 클러스터의 갯수를 할당한다.
	 * @param Context context: 하둡 시스템과 MapReduce 사이의 상호 작용 인자
	 * @author Moonie
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		
		mDelimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
		
		mIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		mNominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		mExceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
		
		mClusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));
	}
	
    /**
     * Mapper에서 받은 클러스터 번호에 입력 데이터를 할당 후 입력 데이터 갯수를 활용하여 중심점 정보를 변경한다.
     * @param IntWritable key   : 클러스터 번호
     * @param Iterable<Text> values : 입력 데이터
     * @param Context context : 하둡 시스템과 MapReduce 사이의 상호 작용 인자
     * @author Moonie
     */
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			              throws IOException, InterruptedException 
	{
		Iterator<Text> iterator = values.iterator();
	
		KMeansClusterInfoMgr cluster = new KMeansClusterInfoMgr();
		cluster.setClusterID(key.get());
		int dataCnt = 0;
		while (iterator.hasNext())
		{
			dataCnt++;
			String tokens[] = iterator.next().toString().split(mDelimiter);
			
			for(int i=0; i<tokens.length; i++)
			{
                if(!CommonMethods.isContainIndex(mExceptionIndexArr, i, false))
                {
                    if(CommonMethods.isContainIndex(mNominalIndexArr, i, false))
                        cluster.addAttributeValue(i, tokens[i], Constants.DATATYPE_NOMINAL);
                    else if(CommonMethods.isContainIndex(mIndexArr, i, false))
                        cluster.addAttributeValue(i, tokens[i], Constants.DATATYPE_NUMERIC);
                }
			}
		}
		//해당 군집의 각 속성에 대해 평균을 산출.
		cluster.finalCompute(dataCnt);		
		//해당 군집의 내용을 다음의 형태로 출력.
		//군집번호 (구분자 속성번호 구분자 속성 값)
		String writeStr = cluster.getClusterInfoString(mDelimiter);
        context.write(NullWritable.get(), new Text(writeStr));
	}
}
