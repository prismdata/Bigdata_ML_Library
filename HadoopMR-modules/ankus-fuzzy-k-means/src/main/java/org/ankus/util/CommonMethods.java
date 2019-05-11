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
 * 알고리즘 수행 파라미터 검증과 문자열 행태의 파라미터를 배열로 변경.
 * @version 0.0.1
 * @date : 2016.12.06
 * @author HongJoong.Shin
 */
public class CommonMethods {
	private Logger logger = LoggerFactory.getLogger(CommonMethods.class);
	/**
	 * 배열 형태의 인덱스 목록에서 특정 인덱스가 포함되어 있는지 검사함.
	 * @date : 2016.12.06
	 * @author HongJoong.Shin
	 * @param indexArr : 입력 인덱스 배열 
	 * @param index     : 검사할 인덱스 
	 * @return 포함 여부 
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
	 * 배열 형태의 인덱스 목록에서 특정 인덱스가 포함되어 있는지 검사함. 
	 * 배열 인덱스가 -1 하나만 가진 경우 dafaultContain이 true이면 모든 인덱스를 포함, 
	 * false이면 모든 인덱스를 무시함.
	 * @date : 2016.12.06
	 * @author HongJoong.Shin
	 * @param int[] indexArr : 입력 인덱스 배열 
	 * @param int index     : 검사할 인덱스
	 * @param boolean defaultContain : 인덱스 포함 여부
	 * @return boolean : true 포함됨, false 포함되지 않음.
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
	 * 문자열로 구성된 인덱스 목록을 배열로 전환함.
	 * @date : 2016.12.06
	 * @author HongJoong.Shin
	 * @param String strValue :문자열로 구성된 인덱스 목록
	 * @return int [] : 인덱스 목록
	 */
	public static int[] convertIndexStr2IntArr(String strValue)
	{
		String indexStr[] = strValue.split(",");
		int arr[] = new int[indexStr.length];
		for(int i=0; i<indexStr.length; i++)			
		{
			arr[i] = Integer.parseInt(indexStr[i]);
		}
		
		return arr;
	}
	

}
