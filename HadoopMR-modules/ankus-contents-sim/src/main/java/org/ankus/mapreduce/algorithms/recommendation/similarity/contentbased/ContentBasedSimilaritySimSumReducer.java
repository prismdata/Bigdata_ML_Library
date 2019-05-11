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

package org.ankus.mapreduce.algorithms.recommendation.similarity.contentbased;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * 컨텐츠간 유사도를 최종 출력한다.
 * @version 0.1
 * @date : 2013.07.10
 * @author Suhyun Jeon
 */
public class ContentBasedSimilaritySimSumReducer extends Reducer<Text, Text, NullWritable, Text> {

    private String m_delimiter;
    private String m_sumOption;
    private double m_corrValLimit = 0.3;
    /**
     * map을 수행하기 위해 구분자와 유사도 합산 방법, 최소 유사도를 설정한다.
     * @auth Suhyun Jeon
     * @parameter Context context : 하둡 환경 설정 변수
     * @return
     */
	@Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER);
        m_sumOption = conf.get(ArgumentsConstants.SUMMATION_OPTION);
        m_corrValLimit = conf.getDouble(ArgumentsConstants.CORRVALLIMIT, 0.0);
    }

	/**
	 * [입력] Key: 아이템 아이디1 + 구분자 + 아이템 아이디2 , Value : 유사도 + 구분자 + 1 
     * [출력] Key : Null, Value : 아이템1 구분자 아이템2 유사도 + 유사도 계산에 사용된 속성 수.
     * @auth Suhyun Jeon
     * @parameter Text key: 아이템 아이디1 + 구분자 + 아이템 아이디2 
     * @parameter Iterable<Text> values : 유사도 + 구분자 + 1 
     * @parameter Context context : 하둡 환경 설정 변수
     * @return
     */
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        Iterator<Text> iter = values.iterator();

        double sum = 0.0;
        double cnt = 0.0;
        while(iter.hasNext())
        {
            String tokens[] = iter.next().toString().split(m_delimiter);
            double newSum = Double.parseDouble(tokens[0]);
            double n = Double.parseDouble(tokens[1]);
            cnt += n;

            if(m_sumOption.equals(Constants.RECOM_CB_NORMALSUM)) sum += newSum;
            else if(m_sumOption.equals(Constants.RECOM_CB_AVGSUM)) sum += (newSum * n);
            else if(m_sumOption.equals(Constants.RECOM_CB_CFSUM))
            {
                if(sum==0) sum = newSum;
                else if(newSum!=0) sum = sum + newSum - (sum * newSum / 2.0);
            }
        }

        if(m_sumOption.equals(Constants.RECOM_CB_AVGSUM))
        {
            if(sum==0 || cnt == 0) sum = 0;
            else sum = sum / cnt;
        }

        if(sum >= m_corrValLimit)
        {
        	DecimalFormat format = new DecimalFormat("0.###");
        	
            String valueStr = key.toString() + m_delimiter + format.format(sum) + m_delimiter + cnt;
            if(cnt >= 2)
            {
            System.out.println(key.toString());
            System.out.println(format.format(sum) + m_delimiter + cnt);
            }
            context.write(NullWritable.get(), new Text(valueStr));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}