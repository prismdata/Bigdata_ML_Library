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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.ankus.util.ConfigurationVariable;

/**
 * Mapper로 부터 받은 키와 값들을 통하여 기초 통계 연산(min, max, sum,harmonicSum,  을 1차 수행한다.
 * @desc 1st reducer class for numeric statistics computation mr job (2-step)
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats1_2MRSplitReducer extends Reducer<Text, Text, Text, Text>{

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
     * Mapper로 부터 받은 키와 값들을 통하여 
     * 기초 통계 연산(min, max, sum,harmonicSum,  을 1차 수행한다.
     * emit <br>
     * key : 컬럼 번호
     * value :  데이터 수 + 구분자 + 최대 값 + 구분자 + 최소 값 + 구분자 + 합계 + 구분자 + 하모닉 합 + 구분자 + 기하 합 + 구분자 + 스퀘어 합계 + 구분자 + (값이 양수이면 T, 아니면 F)
	 * @param Text key : hashFunction(오프렛, Reducer 갯수, 컬럼 번호)
	 * @param Iterable<Text> values : hashFunction(오프렛, Reducer 갯수, 컬럼 번호) 
     */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
	{
		Iterator<Text> iterator = values.iterator();
						
		long cnt = 0;
		double max = 0;
		double min = 0;
		double sum = 0;
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
				max = value;
				min = value;
			}
			else
			{
				if(max < value) max = value;
				if(min > value) min = value;
			}			
			if(value<=0) allPositive = false;
			sum += value;
			if(allPositive)
			{
				harmonicSum += 1/value;
				geometricSum += Math.log10(value);		// for overflow
			}
			squareSum += Math.pow(value, 2) / 10000;	// for overflow
        }
         String writeValue = cnt + delimiter +
        				max + delimiter +
        				min + delimiter +
        				sum + delimiter +
        				harmonicSum + delimiter +
        				geometricSum + delimiter +
        				squareSum;
        if(allPositive) writeValue += delimiter + "T";
        else writeValue += delimiter + "F";
        		
        context.write(key, new Text(writeValue));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
