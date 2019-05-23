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

package org.ankus.mapreduce.algorithms.recommendation.recommender.userbased;


import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 유사 사용자 정보를 스트리밍으로 로딩하여 유사도 임계값 이상을 가지는 추천 대상 사용자를 획득
 */
public class SimilUserExtractMapper extends Mapper<Object, Text, NullWritable, Text>{

    String m_delimiter;
    String m_similDelimiter;
    String m_targetUID;
    double m_similThreshold;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        //입력 파일의 속성 구분자, 지정하지 않는 경우 기본 '/t'문자로 지정됨
        m_delimiter = conf.get(Constants.DELIMITER);
        
        //유사도 정보가 기록된 파일의 속성 구분자, 지정하지 않는 경우 기본 '\t'문자로 지정됨
        m_similDelimiter = conf.get(Constants.SIMILARITY_DELIMITER); 
        m_targetUID = conf.get(Constants.TARGET_UID);
        m_similThreshold = Double.parseDouble(conf.get(Constants.SIMILARITY_THRESHOLD));
    }
    /**
     * 타겟 사용자의 유사 사용자 정보 로딩<br>
     *@param key = 파일 오프셋, value= 사용자 ID1+유사도구분자+사용자 ID2+유사도구분자 유사도(0~1)
     *emit : 유사 사용자 정보를 스트리밍으로 로딩하여 유사도 임계값 이상을 가지는 추천 대상 사용자를 출력.<br>
     *key :null
     *value: 임계 유사도 이상 사용 + 입력 파일 구분자 + 유사도
     * @return boolean
     * @throws Exception
     */
	@Override
	protected void map(Object key, Text value, Context context)  throws IOException, InterruptedException 
	{
			if(value.toString().trim().length() > 0)
			{				
				String tokens[] = value.toString().split(m_similDelimiter);
			    if(tokens[0].equals(m_targetUID))
		        {
		            if(Double.parseDouble(tokens[2]) >= m_similThreshold)
		                context.write(NullWritable.get(), new Text(tokens[1] + m_delimiter + tokens[2]));
		        }
		        else if(tokens[1].equals(m_targetUID))
		        {
		            if(Double.parseDouble(tokens[2]) >= m_similThreshold)
		                context.write(NullWritable.get(), new Text(tokens[0] + m_delimiter + tokens[2]));
		        }
			}
	}

  

}