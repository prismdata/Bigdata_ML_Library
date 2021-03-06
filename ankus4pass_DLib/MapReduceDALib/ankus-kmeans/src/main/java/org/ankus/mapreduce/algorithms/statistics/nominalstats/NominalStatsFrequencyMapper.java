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

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 설정한 컬럼 번호에 존재하는 범주 데이터에 1을 할당한다.
 * @version 0.0.1
 * @date : 2013.08.20
 * @author Moonie
 */
public class NominalStatsFrequencyMapper extends Mapper<Object, Text, Text, IntWritable>{
	
		private String delimiter;

		private int index; // attribute index for nominal value
		private IntWritable intWritable = new IntWritable(1);
		/**
		 * 구분자를 획득하고, 계산하려는 범주형 데이터의 컬럼 번호를 획득한다.
		 * @param Context context : 하둡 시스템과 MapReduce 사이의 상호 작용 인자
		 * @version 0.0.1
		 * @date : 2013.08.20
		 * @author Moonie
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
			index = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX,  "0"));
		}
	
		/**
		 * 설정한 컬럼 번호에 존재하는 범주 데이터에 1을 할당한다.
		 * @param Object key : 입력 스프릿 오프셋 
		 * @param Text value : 입력 스프릿
		 * @param Context context : 하둡 시스템과 MapReduce 사이의 상호 작용 인자
		 * @version 0.0.1
		 * @date : 2013.08.20
		 * @author Moonie
		 */
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] columns = value.toString().split(delimiter);
			context.write(new Text(columns[index]), intWritable);
 	
			//각 데이터입력마다. MAPCOUNT에 1카운드한다.(전체 데이터의 수를 얻을 수 있다.)
			Counter counter = context.getCounter("NOMINALSTAT","MAPCOUNT");
			counter.increment(1);
		}	
}
