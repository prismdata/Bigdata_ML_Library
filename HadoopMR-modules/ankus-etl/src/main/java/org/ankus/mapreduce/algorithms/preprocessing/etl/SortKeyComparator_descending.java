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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 특정 키를 대상으로 내림 차순을 수행하는 클래스
 * WritableComparator를 사용하여 Hadoop job에서 입력 스플릿에 대해 정렬이 가능하도록 함.
 * @author HongJoong.Shin
 * @date :  2016.12.06
 */

public class SortKeyComparator_descending  extends WritableComparator 
{
	/**
     * Key의 Type을 부모 생성자(WritableComparator)에 설정한다.
     * @author HongJoong.Shin
     * @date :  2016.12.06
     */
    protected SortKeyComparator_descending() 
    {
    	super(Text.class, true);
    }
    /**
     * 입력 문자열이 숫자로 변경 가능한지 검사함.
     * @author HongJoong.Shin
     * @date :  2016.12.06
	 * @parameter String s : 검사할 문자열
	 * @return boolean true: 변환 가능, false : 변환 불가. 
     */ 
    private boolean isNumber(String source)
    {
    	try
    	{
    		double x1 = Double.parseDouble(source) ;
    		return true;
    	}
    	catch(Exception e)
    	{
    		return false;
    	}
    }
    /**
     * 특정 키에 대하여 a,b의 크기 비교, 수치값과 문자값을 모두 지원한다.
     * @author HongJoong.Shin
     * @date :  2016.12.06
	 * @parameter WritableComparable a: 비교 대상 객체.
	 * @parameter WritableComparable b: 비교 대상 객체.
	 * @return 비교 대상 a가 비교 대상 b보다 클 경우 -1을 리턴, 작을 경우 1을 리턴, 같을 경우 0을 리컨. 
     */  
    @Override
    public int compare(WritableComparable a, WritableComparable b) 
    {
    	Text o1 = new Text(a.toString());
        Text o2 = new Text(b.toString());
        
        String string_o1 = o1.toString();
        String string_o2 = o2.toString();
        if(isNumber(string_o1) == true && isNumber(string_o2) == true)
        {
        	double dblo1 = Double.parseDouble(string_o1);
        	double dblo2 = Double.parseDouble(string_o2);
        	DoubleWritable dblwo1 = new DoubleWritable(dblo1);
            DoubleWritable dblwo2 =  new DoubleWritable(dblo2);
        	if(dblwo1.get() < dblwo2.get()) 
            {
                return 1;
            }
            else if(dblwo1.get() > dblwo2.get()) 
            {
                return -1;
            }
            else 
            {
                return 0;
            }
        }
        else
        {
        	if(string_o1.compareTo(string_o2) < 0)
            {
                return 1;
            }
            else if(string_o1.compareTo(string_o2) > 0)
            {
                return -1;
            }
            else 
            {
                return 0;
            }
        }
        
    }
}
