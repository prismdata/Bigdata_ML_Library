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

/**
 * 파일 검증과 인덱스 검사를 수행함.
 * @version 0.0.1
 * @date : 2013.08.23
 * @author Moonie Song
 */
public class CommonMethods {
	private Logger logger = LoggerFactory.getLogger(CommonMethods.class);
	/**
	* indexArray의 요소에 index가 포홤되는지 검사함.
	* @author Moonie Song
	* @param int[] indexArr 
	* @param int index
	* @return true : 포함, false : 포함하지 않음
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
	* indexArray에 -1하나만 있는 경우 defaultContain의 값을 반환함.
	* 그 외의 경우 indexArray의 요소에 index가 포홤되는지 검사함.
	* @author Moonie Song
	* @param int[] indexArr 
	* @param int index
	* @param boolean defaultContain
	* @return true : 포함, false : 포함하지 않음
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
     * 콤마로 구분되어 있는 문자형 인덱스 리스트를 숫자형 인덱스로 변환함.
     * @author Moonie Song
     * @param String strValue 문자형 인덱스 리스트.
     * @return 숫자형 인덱스 배열.
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
    /**
     * filePath로 부터 mapreduce로 생성된 파일을 찾음.
     * @author Moonie Song
     * @parameter FileSystem fs HDFS 파일 시스템 변수
     * @parameter Path filePath 파일이 존재하는 경로
     * @return String mapreduce로 생성된 파일 경로
     */
    public static Path findFile(FileSystem fs, Path filePath) throws Exception
    {
        // TODO: edit findFile,
        /*
            as-is: find file in defined directory
            to-be: find fine in defined path (recursively)
         */
        if(fs.isFile(filePath)) return filePath;
        else
        {
            FileStatus[] status = fs.listStatus(filePath);
            boolean isFile = false;
            for(int i=0; i<status.length; i++)
            {
                Path fPath = status[i].getPath();
                String fNameStr = fPath.getName();

                if((fNameStr.charAt(0)!='.') && (fNameStr.charAt(0)!='_') && fs.isFile(fPath)) return fPath;
            }
            return filePath;
        }
    }

}
