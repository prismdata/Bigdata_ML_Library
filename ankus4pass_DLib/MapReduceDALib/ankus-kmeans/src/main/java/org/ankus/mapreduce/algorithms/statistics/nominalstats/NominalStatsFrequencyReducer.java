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

package org.ankus.mapreduce.algorithms.statistics.nominalstats;

import java.io.IOException;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NominalStatsFrequencyMapper의 전처리 결과값을 기반으로 범주형 데이터의 발생 횟수를 파일에 기록한다.
 * @version 0.0.1
 * @date : 2013.08.20
 * @author Moonie
 */
public class NominalStatsFrequencyReducer extends Reducer<Text, IntWritable, NullWritable, Text>{
	
	private String delimiter;
	private double totalCnt;

	/**
	* NominalStatsFrequencyReducer를 수행하기 위한 기본 설정을 시작한다.
	* @param Context context: 하둡 시스템과 MapReduce 사이의 상호 작용 인자
	* @version 0.0.1
	* @date : 2013.08.20
	* @author Moonie
	*/
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        
				//NominalStatsFrequencyMapper에서 받은 전체 데이터의 갯수를 Counter를 통해 받는다.
        totalCnt = (double)context.getCounter("NOMINALSTAT","MAPCOUNT").getValue();
    }
	/**
	 * 범주형 데이터인 key의 값과 values들을 이용하여 범주형 데이터의 발생 횟수를 파일에 기록한다.
	 * @param Text key : 범주형 키 값
	 * @param Iterable<IntWritable> values : 1이 저장된 IntWritable 변수
	 * @param Context context : 하둡 시스템과 MapReduce 사이의 상호 작용 인자
	 * @version 0.0.1
	 * @date : 2013.08.20
	 * @author Moonie
	*/
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Iterator<IntWritable> iterator = values.iterator();
		
		long sum = 0;
		while (iterator.hasNext()) 
		{
			sum += iterator.next().get();
		}
		context.write(NullWritable.get(), new Text(key.toString() + delimiter + sum));
	}

}
