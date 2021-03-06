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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 통계 데이터 전처리 클래스.
 * 
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats1_2MRMergeMapper extends Mapper<Object, Text, Text, Text>{


	/**
	 * 1단계 기초 통계 자료를 로드하여 컬럼번호, 통계 자료를 key, value로 구조화 한다.
	 * emit <br>
	 * key : 컬럼 번호
	 * value : 컬럼 key의 데이터 수,해당 컬럼 데이터수, 최대 값, 최소 값, 합계, 하모닉 합, 기하 합, 스퀘어 합계, (값이 양수이면 T, 아니면 F) 
	 * @param Text key : 오프셋
	 * @param Text value :hash화된 컬럼 번호 \t 컬럼 key의 데이터 수,해당 컬럼 데이터수, 최대 값, 최소 값, 합계, 하모닉 합, 기하 합, 스퀘어 합계, (값이 양수이면 T, 아니면 F)
	 * @param Context context : MapReduce 환경 변수.
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String valueStr = value.toString();

		int splitIndex = valueStr.indexOf("\t"); //입력 데이터에서 컬럼 번호와 통계 치를 분리하기 위해 고정된 구분자(\t)의 위치를 찾는다. 
		String keyStr = valueStr.substring(0, splitIndex); //컬럼 정보 별도 추출
		keyStr = keyStr.substring(0, keyStr.indexOf("_")); //컬럼 정보에서 번호 부분 추출
		
		valueStr = valueStr.substring(splitIndex + 1);
		//데이터 형성
		/*
		* key : 컬럼 번호
		* value : 컬럼 key의 데이터 수,해당 컬럼 데이터수, 최대 값, 최소 값, 합계, 하모닉 합, 기하 합, 스퀘어 합계, (값이 양수이면 T, 아니면 F) 
		*/		
		context.write(new Text(keyStr), new Text(valueStr));
	}

}
