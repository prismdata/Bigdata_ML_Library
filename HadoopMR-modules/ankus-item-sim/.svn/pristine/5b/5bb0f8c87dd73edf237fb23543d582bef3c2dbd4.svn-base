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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 입력 데이터에서 사용자 번호를 제거하고 아이템 번호와 평점을 제구성.<br>
 * Input Split key : Offset , Value:<아이템1 아이템2 사용자 아이템1'평점 아이템2'평점><br>
 * Result data set: Key: <아이템1 @@ 아이템2>, Value:<아이템1'평점 아이템2'평점><br>
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class ItemBasedSimilarityComputeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String m_delimiter;
    
    /**
     * 아이템간 구분자를 획득한다.
     * @author Suhyun Jeon
     * @date 2013.07.20
     * @param Context context:하둡 연결 정보
     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        m_delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER);
    }
    /**
     * 입력 데이터에서 사용자 번호를 제거하고 아이템 번호와 평점을 제구성.<br>
     * Input Split key : Offset  Value:<아이템1 아이템2 사용자 아이템1'평점 아이템2'평점><br>
     * Result data set: Key: <아이템1 @@ 아이템2>, Value:<아이템1'평점 아이템2'평점><br>
     * @param LongWritable key : 입력 스프릿 오프셋
     * @param Text value : <아이템1 아이템2 사용자 아이템1'평점 아이템2'평점>
     * @param Context context :하둡 연결 정보
     * @version 0.0.1
     * @date : 2013.07.20
     * @author Suhyun Jeon
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] tokens = value.toString().split(m_delimiter);

        String keyStr = tokens[0] + "@@" + tokens[1];
        String valueStr = tokens[3] + m_delimiter + tokens[4];
        context.write(new Text(keyStr), new Text(valueStr));
    }

}