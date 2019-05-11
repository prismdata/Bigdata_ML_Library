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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 정렬할 데이터의 키를 설정하여 ETL_SortReducer로 출력한다.
 * @author HongJoong.Shin
 * @date     2016.12.06
 */
public class ETL_SortMapper extends Mapper<LongWritable, Text,  Text, Text>
{
 
	private Logger logger = LoggerFactory.getLogger(ETL_SortMapper.class);
	private  long sort_index = 0;
	
	/**
	 * 정렬할 데이터의 키 인덱스를 획득한다.
	 * 만약 키 값이 0 이하일 경우 0으로 강제 설정한다.
	 * @author HongJoong.Shin
	 * @date     2016.12.06
	 * @parameter Context context : 하둡 환경 변수.
	 */
	@Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    	try
	    {
    		String str_sort_index = context.getConfiguration().get(ArgumentsConstants.ETL_NUMERIC_SORT_TARGET,"0");
    		if(str_sort_index != null)
    		{
    			sort_index = Long.parseLong(str_sort_index);
    			if(sort_index < 0)
    			{
    				sort_index = 0;
    			}
    		}
    		else
    		{
    				sort_index = 0;
    		}
	    }
    	catch(Exception e)
    	{
    		sort_index = 0;
    		logger.error(e.toString());
    	}
    }
	/**
	 * 입력 스플릿을 받아 정렬 대상 키에 해당하는 값을 emit의 키로 설정하고, 
	 * 1개 입력 레코드의 모든 컬럼을 emit의 value로 설정하여 출력한다. 
	 * @author HongJoong.Shin
	 * @date     2016.12.06
	 * @parameter LongWritable key : 입력 스프릿의 오프
	 * @parameter Text value : 입력 스프릿 레코드 
	 * @parameter Context context : 하둡 환경 변수 
	 */
    @Override
    protected void map(LongWritable key, Text value, Context context)   throws IOException, InterruptedException 
    {
        String val = value.toString();        
        String delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
        if (val != null && !val.isEmpty() && val.length() >= 5) 
        {
            String[] splits = val.split(delimiter);
            String outKey = "", outValue = "";
            for(int i = 0; i < splits.length; i++)
            {
            	if(i == sort_index)
            	{
            		outKey = splits[i];
            		break;
            	}            
            }
            outValue = val;        
            context.write(new Text(outKey), new Text(outValue));
        }
    }
}