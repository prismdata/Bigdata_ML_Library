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
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *제목 혹은 아이디와 속성(특성)으로 구분되는 입력 데이터를 속성 인덱스로 구분하여 Key,Value로 제 구성한다.
 * @version 0.1
 * @date : 2013.09.25
 * @author Suhyun Jeon
 */
public class ContentBasedSimilarityAttrSimMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String m_delimiter;
    private int m_indexArray[];
    private int m_exceptionIndexArr[];
    private int m_idIndex;
    private org.slf4j.Logger logger = LoggerFactory.getLogger(ContentBasedSimilarityAttrSimMapper.class);
    /**
     * map함수를 수행하기 위해 구분자, 속성 인덱스, 아이템 아이디 인덱스를 설정한다.
     * @auth Suhyun Jeon
     * @parameter Context context
     * @return
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        //유사도 분석에 사용할 인덱스 목록
        m_indexArray = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX, "-1"), ",");
        //인덱스 목록에서 제외할 목록
        m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.EXCEPTION_INDEX, "-1"), ",");
        //아이템 아이디가 위치한 인덱스
        m_idIndex = Integer.parseInt(conf.get(ArgumentsConstants.KEY_INDEX, "0"));
    }
   
    /**
     * 제목 혹은 아이디와 속성(특성)으로 구분되는 입력 데이터를 속성 인덱스로 구분하여 Key,Value로 제 구성한다.
     *  [입력] 아이템 아이디, 제목, 장르 {1,2,3,..,}
     *  [출력] Key: attr-인덱스 Value:(컨텐츠 분류명1+구분자2+컨텐츠 분류명2,...)
     * @auth Suhyun Jeon
     * @parameter LongWritable key : 데이터 오프셋
     * @parameter Text value : 입력 데이터
     * @parameter Context context : 하둡 환경 설정 변수
     * @return
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] tokens = value.toString().split(m_delimiter);
        for(int i=0; i<tokens.length; i++)
        {
            if(CommonMethods.isContainIndex(m_indexArray, i, true)
                    && !CommonMethods.isContainIndex(m_exceptionIndexArr, i, false)
                    && (m_idIndex != i))
            {
                String keyStr = "attr-" + i;
                String valueStr = tokens[m_idIndex] + m_delimiter + tokens[i];
                context.write(new Text(keyStr), new Text(valueStr));
            }
        }
    }

}