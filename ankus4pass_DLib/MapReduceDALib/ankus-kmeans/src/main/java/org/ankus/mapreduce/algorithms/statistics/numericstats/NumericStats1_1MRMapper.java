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

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 수치 통계를 산출하기 위한 전처리 단계
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats1_1MRMapper extends Mapper<Object, Text, IntWritable, Text>{

	private String delimiter;
    // attribute index array for stat computation
	private int indexArray[];
    // attribute index array for do not computation
	private int exceptionIndexArr[];
	
	/**
	 * 하둡 환경 변수를 이용하여 변수 구분자, 변수 목록, 전체 변수에서 제외할 목록을 획득한다.
	 * @author Moonie
	 * @param Context context : 하둡 환경 변수
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
	 * 입력 데이터에 대하여 컬럼 번호와 해당 번호의 값을 Reducer로 전달함.
	 * @param Object key : 데이터 오프셋
	 * @param Text value : 입력 데이터
     * @param Context context : 하둡 환경 변수
	 * @throws IOException
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] columns = value.toString().split(delimiter);
		
		for(int i=0; i<columns.length; i++)
		{
			if(CommonMethods.isContainIndex(indexArray, i, true) && !CommonMethods.isContainIndex(exceptionIndexArr, i, false))
			{	
				context.write(new IntWritable(i), new Text(columns[i]));
			}
		}
	}
}
