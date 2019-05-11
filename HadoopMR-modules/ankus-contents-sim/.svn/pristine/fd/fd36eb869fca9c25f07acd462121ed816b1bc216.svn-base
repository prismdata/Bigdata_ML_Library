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
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 아이템간 유사도를 Key Value로 분리하는 전처리 작업을 수행한다.
 * @version 0.1
 * @date : 2013.11.05
 * @author Suhyun Jeon
*/
public class ContentBasedSimilaritySimSumMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String m_delimiter;
    private String m_sumOption;

    /**
     * map을 수행하기 위해 구분자와 유사도 합산 방법을 설정한다.
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
    }

    /**
     * 아이템간 유사도를 Key Value로 분리하는 전처리 작업을 수행한다.
     * [입력]  아이템 아이디1 + 아이템 아이디2 + 속성 번호 + 유사도 +  1
     * [출력] Key: 아이템 아이디1 + 구분자 + 아이템 아이디2 , Value : 유사도 + 구분자 + 1 
     * @auth Suhyun Jeon
     * @parameter LongWritable key : 데이터 오프셋
     * @parameter Text value : 아이템 아이디1 + 아이템 아이디2 + 속성 번호 + 유사도 +  1
     * @parameter Context context : 하둡 환경 설정 변수
     * @return
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
    	String tokens[] = value.toString().split(m_delimiter);

        String keyStr = tokens[0] + m_delimiter + tokens[1];
        String valueStr = tokens[3] + m_delimiter + tokens[4];
        context.write(new Text(keyStr), new Text(valueStr));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}