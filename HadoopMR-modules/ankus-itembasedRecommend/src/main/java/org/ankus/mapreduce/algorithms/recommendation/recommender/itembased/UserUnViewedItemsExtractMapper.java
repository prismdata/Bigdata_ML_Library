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

package org.ankus.mapreduce.algorithms.recommendation.recommender.itembased;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 입력 데이터에서 사용자가 구매하지 않은 아이템을 추출하는 클래스
 * @author Wonmoon
 */
public class UserUnViewedItemsExtractMapper extends Mapper<Object, Text, NullWritable, Text>{

    String m_delimiter;
    String m_targetUID;
    int m_uidIndex;
    int m_iidIndex;
    int m_ratingIndex;
    ArrayList<String> m_userViewedItemList;

    /**
     * map()함수를 수행하기 위해 필요한 인자들을 얻고
     * 사용자가 구매한 아이템을 로드한다.
     * @author Wonmoon
     * @param Context context : 하둡 환경 설정 변수
     * @return void
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER);
        m_targetUID = conf.get(ArgumentsConstants.TARGET_UID);
        m_uidIndex = Integer.parseInt(conf.get(ArgumentsConstants.UID_INDEX));
        m_iidIndex = Integer.parseInt(conf.get(ArgumentsConstants.IID_INDEX));
        m_ratingIndex = Integer.parseInt(conf.get(ArgumentsConstants.RATING_INDEX));

        String viewList[] = conf.get(Constants.RECOMJOB_USERS_VIEWED_INFOS).split(m_delimiter);
        m_userViewedItemList = new ArrayList<String>();
        for(int i=0; i<viewList.length; i++)
        {
            m_userViewedItemList.add(viewList[i+1]);
            i += 2;
        }
    }

    /**
     * 평점 정보로 부터 사용자가 구매하지 않은 아이템만 추출한다.
     * [입력] 사용자, 아이템, 평점
     * [출력] 아이템 
     * @author Wonmoon
     * @param Object key : 데이터 오프셋
     * @param Text value : 입력 데이터 레코드
     * @param Context context : 하둡 환경 설정 변수
     * @return void
     */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
        String tokens[] = value.toString().split(m_delimiter);

        if(!tokens[m_uidIndex].equals(m_targetUID) && !m_userViewedItemList.contains(tokens[m_iidIndex]))
        {
            context.write(NullWritable.get(), new Text(tokens[m_iidIndex]));
        }
	}
}
