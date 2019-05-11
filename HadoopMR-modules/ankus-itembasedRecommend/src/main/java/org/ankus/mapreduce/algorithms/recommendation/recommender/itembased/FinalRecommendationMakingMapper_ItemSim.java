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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * 유사도 정보를 통하여 사용자가 구매한 아니템과 연관성이 높은 아이템의 평점과 유사도를 출력하는 클래스
 * @author Wonmoon
 */
public class FinalRecommendationMakingMapper_ItemSim extends Mapper<Object, Text, Text, Text>{

    String m_delimiter;
    String m_similDelimiter;

    HashMap<String, Double> m_userViewListMap= null;
    ArrayList<String> m_targetItemList = null;

    /**
     * 사용자가 구매한 아이템과 평점을 HashMap에 저장하고
     * 비교 대상 아이템을 리스트에 저장한다. 만약 별도의 대상 아이템을 선정하지 않으면 
     * 분산 캐쉬에 등록된 구매 하지 않은 모든 아이템을 읽어온다.
     * @author Wonmoon
     * @param Context context : 하둡  환경 설정 변수
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER);
        m_similDelimiter = conf.get(ArgumentsConstants.SIMILARITY_DELIMITER);

        m_userViewListMap = new HashMap<String, Double>();
        String viewList[] = conf.get(Constants.RECOMJOB_USERS_VIEWED_INFOS).split(m_delimiter);
        for(int i=0; i<viewList.length; i++)
        {
        	//KEY : ITEM ID, VALUE : RATING
            m_userViewListMap.put(viewList[i+1], Double.parseDouble(viewList[i+2]));
            i += 2;
        }
        m_targetItemList = new ArrayList<String>();
        if(conf.get(Constants.RECOMJOB_ITEM_DEFINED).equals("true"))
        {
            String items[] = conf.get(ArgumentsConstants.TARGET_IID_LIST).split(m_delimiter);
            for(String item: items) m_targetItemList.add(item);
        }
        else
        {
            FileSystem fs = FileSystem.getLocal(conf);
            Path[] targetPathArr = DistributedCache.getLocalCacheFiles(conf);
            for(Path p: targetPathArr)
            {
                FSDataInputStream fin = fs.open(p);
                BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
                String readStr;
                while((readStr=br.readLine())!=null)
            	{
                	m_targetItemList.add(readStr);
            	}
                br.close();
                fin.close();
            }
        }
    }

    /**
     * 유사도 데이터 중에 사용자 아이템이 포함되어 있다면 유사한 아이템 사용자가 구배한 아이템의 평점과 아이디 유사도를 출력한다.
     * [입력] 연관 아이템1, 연관 아이템2, 유사도
     * [출력] Key : 연관 아이템, Value : 사용자가 구매한 아이템의 평점 + 사용자가 구매한 아이템 아이디 + 유사도
     * @author Wonmoon
     * @param Object key
     * @param Text value : (유사도 데이터) 아이템 아이디1, 아이템 아이디2, 유사도 점수
     * @param Context context 하둡 환경 설정 변수.
     * @return
     */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		/*
		 * 2017-12-12
		 * whitepoo
		 * 유사도 데이터의 구분자 변경
		 * 입력파일 구분자->유사도 파일 구분자.
		 */
//        String tokens[] = value.toString().split(m_delimiter);
        String tokens[] = value.toString().split(m_similDelimiter);
        String Similar_Item1 = tokens[0];
        String Similar_Item2 = tokens[1];
        String Similarity = tokens[2];
        
        //유사도 데이터에 사용자가 본 아이템이 있고, 추천 대상 아이템이 있다면.
        if(m_userViewListMap.containsKey(Similar_Item2) && m_targetItemList.contains(Similar_Item1))
        {
            double UserView_Item_Rating = m_userViewListMap.get(Similar_Item2);
            String valueStr = UserView_Item_Rating + m_delimiter + Similar_Item2+ m_delimiter +Similarity;
            //Key : 연관 아이템, Value : 사용자가 구매한 아이템의 평점 + 사용자가 구매한 아이템 아이디 + 유사도
            context.write(new Text(Similar_Item1), new Text(valueStr));
        }
        else if(m_userViewListMap.containsKey(Similar_Item1) && m_targetItemList.contains(Similar_Item2))
        {
        	double UserView_Item_Rating  = m_userViewListMap.get(Similar_Item1);
        	String valueStr = UserView_Item_Rating + m_delimiter + Similar_Item1 + m_delimiter +Similarity;
        	//Key : 연관 아이템, Value : 사용자가 구매한 아이템의 평점 + 사용자가 구매한 아이템 아이디 + 유사도
            context.write(new Text(Similar_Item2), new Text(valueStr));
        }
	}
}
