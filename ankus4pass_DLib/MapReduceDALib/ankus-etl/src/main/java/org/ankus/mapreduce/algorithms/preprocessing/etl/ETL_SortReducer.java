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

package org.ankus.mapreduce.algorithms.preprocessing.etl;

import java.io.IOException;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * ETL_SortMapper로 부터 Key에 따라 정렬된 레코드들을 받아 파일로 출력한다.
 * 파일의 각 컬럼은 사용자 정의 구분자로 분리된다.
 * @author HongJoong.Shin
 * @date     2016.12.06
 */
public class ETL_SortReducer  extends Reducer<Text, Text, Text, DoubleWritable>
{
	private Logger logger = LoggerFactory.getLogger(ETL_SortReducer.class);
	String delimiter = "";
	/**
	 * 사용자 설정에 의한 각 컬럼의 구분자를 획득한다. 출력시 해당 구분자를 기준으로 컬럼이 구분된다.
	 * @auth HongJoong.Shin
	 * @date     2016.12.06
	 */
	protected void setup(Context context) throws IOException, InterruptedException
    {
		delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }

	/**
	 * ETL_SortMapper로 부터 Key에 따라 정렬된 레코드들을 받아 파일로 출력한다.
	 * @author HongJoong.Shin
	 * @date   2016.12.06
	 * @parameter Text value : 정렬 대상 키.
	 * @parameter Iterable<Text> value : 스트림 형태의 다수의 레코드 
	 * @parameter Context context : 하둡 환경 변수 
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)  throws IOException, InterruptedException
	{
		for(Text val : value) 
		{
			logger.info(new Text(key + delimiter + val).toString());
			context.write(new Text(val), null);
		}
	}
}