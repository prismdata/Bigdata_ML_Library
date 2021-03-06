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

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 4개 구간으로 분리하기 위해 데이터에 4분위 구간 정보를 데이터에 첨부하여 출력한다.
 * @desc mapper class for numeric statistics computation mr job (1-step)
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats2_BlockInfoMapper extends Mapper<Object, Text, NullWritable, Text>{

	private String delimiter;
    // attribute index array for stat computation
	private int indexArray[];
    // attribute index array for do not computation
	private int exceptionIndexArr[];
	/**
     * 하둡 환경 변수를 이용하여 변수 구분자, 변수목록, 전체 변수에서 제외할 변수 목록을 획득한다.
     * @author Moonie
     * @param  Context context : 하둡 환경 변수
     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // TODO
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
        indexArray = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX,  "-1"));
        exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
    }

    /**
     * 입력 데이터를 4개 구간으로 분리하기 위해 데이터에 구간 위치 정보를 마킹한다.(전처리)
     * input : 입력 데이터
     * output : 컬럼 번호 + "-1,2,3,4B" + 구분자 + 입력 데이터의 컬럼 값
     * @param Object key : 데이터의 오프셋.
     * @param Text value : 입력 데이터 레코드
     * @throws Exception
     */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] columns = value.toString().split(delimiter);
		
		for(int i=0; i<columns.length; i++)
		{
			if(CommonMethods.isContainIndex(indexArray, i, true) && !CommonMethods.isContainIndex(exceptionIndexArr, i, false))
			{
//				입력 데이터의 각 컬럼에 대해 최소, 평균, 최대값 중 위치할 곳을 마킹함.
                String blockInfo[] = context.getConfiguration().get(i + "Block").split(",");
                double val = Double.parseDouble(columns[i]);
                if(val < Double.parseDouble(blockInfo[0]))
                {
                    context.write(NullWritable.get(), new Text(i + "-1B" + delimiter + val));
                    Counter counter = context.getCounter(Constants.STATS_NUMERIC_QUARTILE_COUNTER, i + "-1B");
                    counter.increment(1);
                }
                else if(val < Double.parseDouble(blockInfo[1]))
                {
                    context.write(NullWritable.get(), new Text(i + "-2B" + delimiter + val));
                    Counter counter = context.getCounter(Constants.STATS_NUMERIC_QUARTILE_COUNTER, i + "-2B");
                    counter.increment(1);
                }
                else if(val < Double.parseDouble(blockInfo[2]))
                {
                    context.write(NullWritable.get(), new Text(i + "-3B" + delimiter + val));
                    Counter counter = context.getCounter(Constants.STATS_NUMERIC_QUARTILE_COUNTER, i + "-3B");
                    counter.increment(1);
                }
                else
                {
                    context.write(NullWritable.get(), new Text(i + "-4B" + delimiter + val));
                    Counter counter = context.getCounter(Constants.STATS_NUMERIC_QUARTILE_COUNTER, i + "-4B");
                    counter.increment(1);
                }
			}
		}
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
