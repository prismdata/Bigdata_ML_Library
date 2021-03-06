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

package org.ankus.mapreduce.algorithms.statistics.numericstats;

import java.io.IOException;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Map함수의 전처리 결과로 부터 평균 , 편차, 표준편차, 평군, 산술 평균, 조화 평균, 기하 평균, 최대 , 최소, 전체 데이터의 갯수를 산출한다.
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats1_2MRMergeReducer extends Reducer<Text, Text, NullWritable, Text>{

	private String delimiter;
	/**
     * 하둡 환경 변수를 이용하여 변수 구분자를 획득한다.
     * @author Moonie
     * @date 2013.08.21
     * @param  Context context : 하둡 환경 변수
     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // TODO
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }

	/**
	 * Map함수의 전처리 결과로 부터 합, 평균, 조화평균,기하 평균, 분산, 표준 편차, 최대, 최소, 데이터 수 산출.
	 * emit <br>
	 * key: null
	 * value : 컬럼 번호 \t 합, 평균, 조화평균,기하 평균, 분산, 표준 편차, 최대, 최소, 데이터 수.
	 * @param Text key : 컬럼 번호
	 * @param Text value : 컬럼 key의 데이터 수,해당 컬럼 데이터수, 최대 값, 최소 값, 합계, 조화 합, 기하 합, 스퀘어 합계, (값이 양수이면 T, 아니면 F) 
	 * @param Context context : MapReduce 환경 변수.
     */    
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
	{
		long cnt = 0;
		double max = 0;
		double min = 0;
		double sum = 0;
		double harmonic_sum = 0;
		double geometric_sum = 0;
		double square_sum = 0;			
		boolean allPositive = true;
		
		Iterator<Text> iterator = values.iterator();		
		while (iterator.hasNext()) 
        {
			String tokens[] = iterator.next().toString().split(delimiter);
			
			long curCnt = Long.parseLong(tokens[0]);
			double curMax = Double.parseDouble(tokens[1]);
			double curMin = Double.parseDouble(tokens[2]);
			cnt += curCnt;
        	if(cnt==curCnt)
			{
				max = curMax;
				min = curMin;
			}
			else
			{
				if(max < curMax) max = curMax;
				if(min > curMin) min = curMin;
			}
			
			if(tokens[7].equals("F")) allPositive = false;
			sum += Double.parseDouble(tokens[3]);
			if(allPositive)
			{
				harmonic_sum += Double.parseDouble(tokens[4]);
				geometric_sum += Double.parseDouble(tokens[5]);
			}
			square_sum += Double.parseDouble(tokens[6]);
        }
		
		double avg = sum / (double)cnt;
		double avg_harmonic = 0;
		double avg_geometric = 0;
		if(allPositive)
		{
			avg_harmonic = (double)cnt / harmonic_sum;
			avg_geometric = Math.pow(10, geometric_sum /(double)cnt);
		}
		
		double variance = (square_sum * 10000 /(double)cnt) - Math.pow(avg,2);
		double stdDeviation = Math.sqrt(variance);		

		String writeVal = sum + delimiter +
							avg + delimiter +
							avg_harmonic + delimiter +
							avg_geometric + delimiter +
							variance + delimiter +
							stdDeviation + delimiter +
							max + delimiter +
							min + delimiter +
                            cnt;
		context.write(NullWritable.get(), new Text(key.toString() + delimiter + writeVal));
	}

}
