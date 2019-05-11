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

package org.ankus.mapreduce.algorithms.recommendation.similarity.Userbased;

import java.io.IOException;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *사용자들의 평점 정보 구성 클래스.<br>

 * CFBasedSimilarityComputeMapper
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      1. Cosine coefficient 2. Pearson coefficient
 *      Required data set : [userID, itemID, rating]
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class CFBasedSimilarityComputeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String m_delimiter;
  
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        m_delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER);
    }
/**
 * 사용자들의 평점 정보 Key Value로 구성함
 * @param input user1 \t user2 \t item \t rate1 \t rate2
* <br>emit  key: user1@@user2 value: rate1\rate2
 * @return void
 */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] tokens = value.toString().split(m_delimiter);

        String keyStr = tokens[0] + "@@" + tokens[1];
        String valueStr = tokens[3] + m_delimiter + tokens[4];
        context.write(new Text(keyStr), new Text(valueStr));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}