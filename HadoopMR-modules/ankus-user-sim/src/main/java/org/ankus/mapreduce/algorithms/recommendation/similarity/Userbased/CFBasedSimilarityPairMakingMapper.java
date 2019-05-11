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
 *아이템, 사용자, 평점 Key Value로 구성하는 클래스.<br>
 * CFBasedSimilarityPairMakingMapper
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      1. Cosine coefficient 2. Pearson coefficient
 *      Required data set : [userID, itemID, rating]
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class CFBasedSimilarityPairMakingMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String m_delimiter;
    private int m_uidIndex;
    private int m_iidIndex;
    private int m_ratingIndex;
	private String m_basedType;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER);
        m_iidIndex = Integer.parseInt(conf.get(ArgumentsConstants.UID_INDEX));
        m_iidIndex = Integer.parseInt(conf.get(ArgumentsConstants.IID_INDEX));
        m_ratingIndex = Integer.parseInt(conf.get(ArgumentsConstants.RATING_INDEX));
        m_basedType = conf.get(ArgumentsConstants.BASED_TYPE);
    }
/**
 *사용자, 아이템, 평점으로 구성된. 평점 정보를 입력받아. Key(아이템), Value(사용자, 평점)으로 구성함.
 * @param offset, 문자열
 * @return void
* <br>emit : Key(아이템), Value(사용자, 평점)
 * @throws IOException, InterruptedException
 */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] tokens = value.toString().split(m_delimiter);

        String keyStr = "";
        String valueStr = "";

        keyStr = tokens[m_iidIndex];
        valueStr = tokens[m_uidIndex] + m_delimiter + tokens[m_ratingIndex];
       	context.write(new Text(keyStr), new Text(valueStr));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
