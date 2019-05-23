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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * 전체 사용자+아이템+평점 데이터를 읽고 추천 대상 사용자, 유사 사용자, 아이템, 평점 값을 비교하여
 * 추천 할 아이템을 추출하는 클래스.
 */
public class FinalRecommendationMakingMapper_UserSim extends Mapper<Object, Text, Text, Text>{

    String m_delimiter;
    int m_uidIndex;
    int m_iidIndex;
    int m_ratingIndex;
    String m_targetUID;
    HashMap<String, Double> m_similUserMap = null;

    boolean m_isTargetItemListDefined = false;
    String m_targetIIDList[] = null;
    ArrayList<String> m_userViewedItemList = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(Constants.DELIMITER);
        m_uidIndex = Integer.parseInt(conf.get(Constants.UID_INDEX));
        m_iidIndex = Integer.parseInt(conf.get(Constants.IID_INDEX));
        m_ratingIndex = Integer.parseInt(conf.get(Constants.RATING_INDEX));
        m_targetUID = conf.get(Constants.TARGET_UID);
        
        //유사 사용자 정보를 구분자로 분리함.
        String similUserStr[] = conf.get(Constants.RECOMJOB_SIMIL_USER_INFOS).split(m_delimiter);
        m_similUserMap = new HashMap<String, Double>();
        for(int i=0; i<similUserStr.length; i++)
        {
        	//key : 유사 사용자, value: 유사도.
            m_similUserMap.put(similUserStr[i], Double.parseDouble(similUserStr[i+1]));
            i++;
        }

//      추천할 아이템이 있는 경우.
        if(conf.get(Constants.RECOMJOB_ITEM_DEFINED).equals("true")) m_isTargetItemListDefined = true;
        else m_isTargetItemListDefined = false;

        if(m_isTargetItemListDefined) m_targetIIDList = conf.get(Constants.TARGET_IID_LIST).split(",");
        else
        {
        	//추천 대상에 대한 레코드 결합.
            String viewList[] = conf.get(Constants.RECOMJOB_USERS_VIEWED_INFOS).split(m_delimiter);
            m_userViewedItemList = new ArrayList<String>();
            for(int i=0; i<viewList.length; i++)
            {
            	//추천 대상이 본 아이템만 추출.
                m_userViewedItemList.add(viewList[i+1]);
                i += 2;
            }
        }
    }
/**
 *@param 사용자+구분자+평점을 포함한 문자열.
 *@emit key: 아이템, value :평점+사용자+유사 사용자에 대한 유사도.
 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
        String tokens[] = value.toString().split(m_delimiter);
      
        //입력 받은 데이터의 사용자가 추천 대상 사용자 이며, 사용자간 유사도 정보에 포함되있는지 확인.
        if(!tokens[m_uidIndex].equals(m_targetUID) && m_similUserMap.containsKey(tokens[m_uidIndex]))
        {
        	//추천 대상 아이템이 정의된 경우.
            if(m_isTargetItemListDefined)
            {
                for(String item: m_targetIIDList)
                {
                    if(item.equals(tokens[m_iidIndex]))
                    {
                        String keyStr = tokens[m_iidIndex];
                        String valueStr = tokens[m_ratingIndex] + m_delimiter + tokens[m_uidIndex] + m_delimiter + m_similUserMap.get(tokens[m_uidIndex]);
                        context.write(new Text(keyStr), new Text(valueStr));
                        break;
                    }
                }
            }
            else
            {
            	//추천 대상 아이템이 정의되지 않은 경우.
                if(!m_userViewedItemList.contains(tokens[m_iidIndex]))
                {
                	//유사한 사용자가 본 아이템, 평점, 사용자간 유사도를 출력
                	//Key : 아이템 ID
                	//Value : 평점 + 추천 대상 사용자 + 유사 사용자의 유사도.
                    String keyStr = tokens[m_iidIndex];
                    String valueStr = tokens[m_ratingIndex] + m_delimiter + tokens[m_uidIndex] + m_delimiter + m_similUserMap.get(tokens[m_uidIndex]);
                    context.write(new Text(keyStr), new Text(valueStr));
                }
            }
        }
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {

    }

}