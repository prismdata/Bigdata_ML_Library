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

package org.ankus.mapreduce.algorithms.preprocessing.normalize;

import java.io.IOException;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 정규를 진행하는 Mapper클래스.
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NormalizeMapper extends Mapper<Object, Text, NullWritable, Text>{

    // delimiter for attribute separation
	private String mDelimiter;
    // attribute index array for normalization
	private int mIndexArr[];
    // value for if no-normalization attributes is remain
	private boolean mRemainFields;
    // attribute index array for do not normalize
	private int mExceptionIndexArr[];
	/**
	 * Mapper를 수행하기위한 정보(구분자, 정규화 대상 컬럼,제외 컬럼, 대상 이외 컬럼  출력 유지 여부)를 설정한다.
	 * @version 0.0.1
	 * @param Context  context: MapReduce 설정 변수  
	 * @return
	 * @throws Exception
	 */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        mDelimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
        mIndexArr = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX,  "-1"));
        mExceptionIndexArr = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));

        if(context.getConfiguration().get(ArgumentsConstants.REMAIN_FIELDS, "true").equals("true")){
            mRemainFields = true;
        }
        else
        {
            mRemainFields = false;
        }
    }
	/**
	 * 입력 스프릿을 받아 최대/최소 정규화를 수행한다.<br>
	 * 정규화는 입력값 - 최소값 / 최대값 - 최소값 을 수행하는 것으로 진행한다.
	 * @param Object key: MapReduce 설정 변수  
	 * @param Text value: MapReduce 설정 변수  
	 * @param Context  context: MapReduce 설정 변수  
	 * @return
	 * @throws Exception
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] columns = value.toString().split(mDelimiter);
		double point_ = Math.pow(10, 3);
		String writeVal = "";
		for(int i=0; i<columns.length; i++)
		{
			if(CommonMethods.isContainIndex(mIndexArr, i, true))
			{
				if(!CommonMethods.isContainIndex(mExceptionIndexArr, i, false))
				{
					if(i>0) writeVal += mDelimiter;
					
					String minMax[] = context.getConfiguration().get(Constants.STATS_MINMAX_VALUE + "_" + i, "0,0").split(",");
					
					double val1 = Double.parseDouble(columns[i])-Double.parseDouble(minMax[0]);							
					double val2 = Double.parseDouble(minMax[1])-Double.parseDouble(minMax[0]);
					
					if((val2==0)||(val2==0)){
						writeVal += "0";
					}
					else {
						double div = Math.round((val1 / val2) * point_)/ point_;
						writeVal += "" + div;
					}
				}
				else if(mRemainFields)
				{
					if(i > 0) writeVal += mDelimiter;
					writeVal += columns[i];
				}
			}
			else if(mRemainFields)
			{
				if(i>0) writeVal += mDelimiter;
				writeVal += columns[i];
			}
		}
		context.write(NullWritable.get(), new Text(writeVal.trim()));
	}

}
