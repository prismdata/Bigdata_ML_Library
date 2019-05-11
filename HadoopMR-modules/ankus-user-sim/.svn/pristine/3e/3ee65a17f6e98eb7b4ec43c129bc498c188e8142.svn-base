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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 아이템을 공유한 사용자 점수 쌍을 생성.
 * (사용자1, 사용자2) (아이템), (평점1, 평점2)
 * CFBasedSimilarityPairMakingReducer
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      1. Cosine coefficient 2. Pearson coefficient
 *      Required data set : [userID, itemID, rating]
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class CFBasedSimilarityPairMakingReducer extends Reducer<Text, Text, NullWritable, Text> {

    private String m_delimiter;
    private String m_targetID;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER);
        m_targetID = conf.get(ArgumentsConstants.TARGET_ID);
    }

    /**
     * Mapper로 부터 받은  아이템 , 사용자, 평점을 이용하여 아이템을 공유한 사용자들의 평점들을 구성함.
     * @param key: 아이템,  Values: 사용자, 평점
     * @return void
* <br>emit  key(null), value(user1 \t user2 \t item \t rate1 \t rate2)
     * @throws IOException, InterruptedException
     */
    @Override
    protected void reduce(Text item, Iterable<Text> user_rate, Context context) throws IOException, InterruptedException {

        Iterator<Text> iter = user_rate.iterator();
        ArrayList<String> valueList = new ArrayList<String>();

        while (iter.hasNext()) valueList.add(iter.next().toString());

        for(int i=0; i<valueList.size(); i++)
        {
            String iStr[] = valueList.get(i).split(m_delimiter);
            for(int k=i+1; k<valueList.size(); k++)
            {
                String kStr[] = valueList.get(k).split(m_delimiter);
                boolean isWrite = true;
                if(!m_targetID.equals("-1"))
                {
                    if(!iStr[0].equals(m_targetID) && !kStr[0].equals(m_targetID)) isWrite = false;
                }

                if(isWrite)
                {
                    String writeStr = "";
                    if(iStr[0].compareTo(kStr[0]) < 0)
                    {
                        writeStr = iStr[0] + m_delimiter + kStr[0] + m_delimiter +
                                item.toString() + m_delimiter + iStr[1] + m_delimiter + kStr[1];
                    }
                    else
                    {
                        writeStr = kStr[0] + m_delimiter + iStr[0] + m_delimiter +
                                item.toString() + m_delimiter + kStr[1] + m_delimiter + iStr[1];
                    }
                   
                    context.write(NullWritable.get(), new Text(writeStr));
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}