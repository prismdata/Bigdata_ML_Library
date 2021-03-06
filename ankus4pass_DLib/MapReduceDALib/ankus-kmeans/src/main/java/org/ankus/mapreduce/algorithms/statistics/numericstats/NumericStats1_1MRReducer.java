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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Mapper로 부터 Key,Value 목록을 기반으로 기본적인 산술 연산을 수행함.
 * @desc reducer class for numeric statistics computation mr job (1-step)
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats1_1MRReducer extends Reducer<IntWritable, Text, NullWritable, Text>{

	private String delimiter;
	/**
	 * 하둡 환경 변수를 이용하여 변수 구분자를 획득한다.
	 * @author Moonie
	 * @param Context context: 하둡 환경 변수
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }
 
    /**
     * Mapper로 부터 Key,Value 목록을 기반으로 기본적인 산술 연산을 수행함.
     * emit <br>
     * key : 컬럼 번호
     * value : 합 + 구분자 + 평균 + 구분자 + 조화 평균  + 구분자 + 기하 평균 + 구분자 +분산 + 구분자 +표준편차 + 구분자 +최대 값 + 구분자 +최소 값 + 구분자 + 전체 데이터 수;
     * @param IntWritable key : 컬럼 번호
     * @param Iterable<Text> values : 컴럼 값 목록
     * @param Context context: 하둡 환경 변수
     */
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		Iterator<Text> iterator = values.iterator();
						
		long cnt = 0;
		double sum = 0;
		double avg = 0;
		double avgGeometric = 0;
		double avgHarmonic = 0;
		double variance = 0;
		double stdDeviation = 0;		
		double maxData = 0;
		double minData = 0;
		double middleData_Value = 0;		
		double harmonicSum = 0;
		double geometricSum = 0;
		double squareSum = 0;
		boolean allPositive = true;
		
        while (iterator.hasNext()) 
        {
        	double value = Double.parseDouble(iterator.next().toString());
        	cnt++;
        	
        	if(cnt==1)
			{
				maxData = value;
				minData = value;
			}
			else
			{
				if(maxData < value) maxData = value;
				if(minData > value) minData = value;
			}
			
			if(value <= 0) allPositive = false;
			sum += value;
			if(allPositive)
			{
				harmonicSum += 1/value;
				geometricSum += Math.log10(value);
			}
			squareSum += Math.pow(value, 2) / 10000;
        }
        avg = sum / (double)cnt;		
		if(allPositive)
		{
			avgHarmonic = (double)cnt / harmonicSum;
			avgGeometric = Math.pow(10, geometricSum / (double)cnt);
		}
		else
		{
			avgHarmonic = 0;
			avgGeometric = 0;
		}
		
		variance = (squareSum * 10000 /(double)cnt) - Math.pow(avg,2);
		stdDeviation = Math.sqrt(variance);		
		middleData_Value = (maxData + minData) / 2;

        String writeVal = sum + delimiter +
				avg + delimiter +
				avgHarmonic + delimiter +
				avgGeometric + delimiter +
				variance + delimiter +
				stdDeviation + delimiter +
				maxData + delimiter +
				minData + delimiter +
                cnt;
        context.write(NullWritable.get(), new Text(key.toString() + delimiter + writeVal));
	}

}
