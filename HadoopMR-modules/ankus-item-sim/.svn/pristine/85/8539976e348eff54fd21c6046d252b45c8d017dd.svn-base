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

package org.ankus.mapreduce.algorithms.recommendation.similarity.Itembased;

import java.io.IOException;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 입력 데이터 스플릿으로 부터 Key(사용자), Value(아이템, 평점)로 제구성하는 Mapper<br>
 * Input split : [userID, itemID, rating]<br>
 * Mapper Output : Key(사용자), Value(아이템, 평점)<br>
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class ItemBasedSimilarityPairMakingMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String m_delimiter;
    private int m_uidIndex;
    private int m_iidIndex;
    private int m_ratingIndex;
    /**
    * 하둡 환경 변수로 부터 변수간 구분자, 사용자 인덱스, 아이템 인덱스, 평점 인덱스를 획득한다
    * @param Context context : 하둡 연결 정보
    * @version 0.0.1
	* @date : 2013.07.20
	* @author Suhyun Jeon
	*/
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER);
        m_uidIndex = Integer.parseInt(conf.get(ArgumentsConstants.UID_INDEX));
        m_iidIndex = Integer.parseInt(conf.get(ArgumentsConstants.IID_INDEX));
        m_ratingIndex = Integer.parseInt(conf.get(ArgumentsConstants.RATING_INDEX));
    }
    /**
    * 평점 데이터에서 key, value로 제구성하여 리듀서에 전달<br>
    * key: 사용자 아이디.<br>
    * value: 아이템 아이디 구분자 평점.<br>
    * @param LongWritable key : 입력 스트릿 오프셋 
    * @param Text value : 평점 데이터 
    * @param Context context : 하둡 연결 정보       
    * @version 0.0.1
	* @date : 2013.07.20
	* @author Suhyun Jeon
    */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] tokens = value.toString().split(m_delimiter);

        String keyStr = "";
        String valueStr = "";

        keyStr = tokens[m_uidIndex];
        valueStr = tokens[m_iidIndex] + m_delimiter + tokens[m_ratingIndex];

	  	context.write(new Text(keyStr), new Text(valueStr));
    }

}
