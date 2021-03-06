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
import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
//import com.jcraft.jsch.Logger;
/**
 * Mapper에서 생성한 결과가 다수의 파일로 나누어지므로 1개의 파일로 결합하기 위해 사용한다.
 * 
 * @author HongJoong.Shin
 * @date   2016.12.06
 */
public class ETL_FilterReducer  extends Reducer<NullWritable, Text, NullWritable, Text>
{
	private Logger logger = LoggerFactory.getLogger(ETL_FilterReducer.class);
	String delimiter = "";
	/**
     * Mapper의 결과를 받아 최종 파일로 출력한다. 
     * @author HongJoong.Shin
	 * @date :  2016.12.06
	 * @parameter NullWritable key Mapper의 key인 null을 가지고 있다.
	 * @parameter  Iterable<Text> Mapper에서 출력한 Text 리스트를 가지고 있다.
	 * @parameter Context context 하둡 환경 설정 변수
     */
	@Override
	protected void reduce(NullWritable key, Iterable<Text> value, Context context)  throws IOException, InterruptedException
	{
		for(Text val : value) 
		{
			context.write(null, new Text(val));
		}
	}
}