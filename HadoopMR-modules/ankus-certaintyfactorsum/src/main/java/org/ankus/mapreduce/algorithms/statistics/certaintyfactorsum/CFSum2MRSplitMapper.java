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

package org.ankus.mapreduce.algorithms.statistics.certaintyfactorsum;

import java.io.IOException;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.ankus.util.CommonMethods;

/**
 * 입력 스프릿으로 부터 n폴드로 데이터를 그룹하여 출력한다.
 * @version 0.0.1
 * @date : 2013.08.20
 * @author Moonie
 */
public class CFSum2MRSplitMapper extends Mapper<Object, Text, Text, Text>{

    // value for data fold (for distributed computation)
	private int foldValue;
    // attribute index array for cf sum computation
    private int indexArray[];
    // attribute index array for do not computation
    private int exceptionIndexArray[];
    private String delimiter;
    /**
     * 리듀서의 갯수, 구분자, 컬럼 인덱스를 획득한다.
     * @author Moonie
     * @date 2013.08.20
     * @param Context context : Job을 설정하는 변수 
     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        foldValue = context.getNumReduceTasks();
        if(foldValue == 0) foldValue = 1;
        //'\t'을 변수명으로 수정해야 함
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
        indexArray = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX,  "-1"));
        exceptionIndexArray = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
    }
    /**
     * Key에 hash 코드를 사용하여 출력될 폴드를 그룹화 시킨다.
     * 
     * @author Moonie
     * @date 2013.08.20
     * @param Object key : 입력 스프릿 오프셋 
     * @param Text value : 입력 스프릿
     * @param Context context  : Job을 설정하는 변수 
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
     */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] columns = value.toString().split(delimiter);
		int outputKey = key.hashCode() % foldValue;
		
		for(int i=0; i<columns.length; i++)
		{
			if(CommonMethods.isContainIndex(indexArray, i, true) && !CommonMethods.isContainIndex(exceptionIndexArray, i, false))
			{
				//key: 컬럼 번호_해쉬값
				//output : 컬럼 값.
				context.write(new Text(i + "_" + outputKey), new Text(columns[i]));
			}
		}
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
