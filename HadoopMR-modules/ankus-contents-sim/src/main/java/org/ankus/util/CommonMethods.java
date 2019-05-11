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

package org.ankus.util;

//import org.ankus.mapreduce.algorithms.classification.knn.kNNDistanceComputeMapper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * 하둡 디렉토리 제어/인덱스 검증  클래스
 * @version 0.0.1
 * @date : 2013.08.23
 * @author Moonie Song
 */
public class CommonMethods {
	private Logger logger = LoggerFactory.getLogger(CommonMethods.class);
	/**
	 * 입력 배열에 대상 인덱스가 존재하는지 검사함.
	 * @auth
	 * @parameter int[] indexArr : 입력 배열
	 * @parameter int index : 대상 인덱스
	 * @return true: 존재함, false: 존재하지 않음.
	 */
	private static boolean isContain(int[] indexArr, int index)
	{
		for(int i: indexArr)
		{
			if(i==index) return true;
		}
		
		return false;
	}
	/**
	 * 입력 배열에 대상 인덱스가 존재하는지 검사함.
	 * 만약  입력 배열에 값 -1이 하나만 있는 경우 defaultContain 값을 리턴한다.
	 * @auth
	 * @parameter int[] indexArr : 입력 배열
	 * @parameter int index : 대상 인덱스
	 * @return true: 존재함, false: 존재하지 않음.
	 */
	public static boolean isContainIndex(int[] indexArr, int index, boolean defaultContain)
	{
		if((indexArr.length == 1) && (indexArr[0] == -1))
		{
			if(defaultContain) return true;
			else return false;
		}
		else return isContain(indexArr, index);
	}
	
	/**
	 * 수치 값으로 표현된 입력 문자열을 특정 문자로 구분하여 정수형 배열로 변환한다.
	 * @auth
	 * @parameter String strValue 특정 문자로 구분되는 정수값 문자열
	 * @parameter String delimiter 구분자 
	 * @return int[] : 정수 배열
	 */
	public static int[] convertIndexStr2IntArr(String strValue, String delimiter)
	{
		String indexStr[] = strValue.split(delimiter);
		int arr[] = new int[indexStr.length];
		for(int i=0; i<indexStr.length; i++)			
		{
			arr[i] = Integer.parseInt(indexStr[i]);
		}
		
		return arr;
	}
		
	
}
