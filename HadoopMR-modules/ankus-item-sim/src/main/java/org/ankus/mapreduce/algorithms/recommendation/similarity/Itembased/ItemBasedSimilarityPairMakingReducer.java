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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 입력 데이터 스플릿으로 부터 Key(사용자), Value(아이템, 평점)로 제구성는 Reducer <br>
 * Input split : Key(사용자), Value(아이템, 평점)<br>
 * Reducer Output : 아이템1 아이템2 사용자 아이템1-평점 아이템2-평점<br>
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class ItemBasedSimilarityPairMakingReducer extends Reducer<Text, Text, NullWritable, Text> {

    private String m_delimiter;
    private String m_targetID;

    /**
    * 하둡 환경 변수로 부터 변수간 구분자, 유사도를 구할 대상 아이템 ID를 획득한다
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
        m_targetID = conf.get(ArgumentsConstants.TARGET_ID);
    }
    /**
    *
    * Mapper에서 배포한 key: 사용자 아이디, value: 아이템 아이디 구분자 평점<br>
    * @param Text key:사용자 아이디
    * @param Iterable<Text> values : [아이템 아이디 구분자 평점],...,[아이템 아이디 구분자 평점]
    * @param Context context : 하둡 연결 정보
    * @version 0.0.1
    * @date : 2013.07.20
    * @author Suhyun Jeon
    */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        ArrayList<String> valueList = new ArrayList<String>();
        while (iter.hasNext()) 
    	{
        	valueList.add(iter.next().toString());
    	}
      
        for(int i=0; i<valueList.size(); i++) //[아이템 아이디 평점]의 연속.
        {
            String iStr[] = valueList.get(i).split(m_delimiter); //아이템 아이디1
            for(int k=i+1; k<valueList.size(); k++)
            {
                String kStr[] = valueList.get(k).split(m_delimiter); //아이템 아이디2
                boolean isWrite = true;
                if(!m_targetID.equals("-1"))
                {
                	//찾으려는 특정한 아이템이 설정된 경우.
                    if(!iStr[0].equals(m_targetID) && !kStr[0].equals(m_targetID)) isWrite = false;
                }
                if(isWrite)
                {
                    //찾으려는 특정한 아이템을 찾았거나 설정되지 않은 경우.
                    String writeStr = "";
                    //아래의 문자 위치 비교 논리는 사용 이유는?
                    if(iStr[0].compareTo(kStr[0]) < 0)
                    {
                    	//서로 다른 아이템인 경우.
                    	//아이템1 아이템2 사용자 아이템1 평점 아이템2 평점으로 Value구성.
                        writeStr = iStr[0] + m_delimiter + kStr[0] + m_delimiter +
                                key.toString() + m_delimiter + iStr[1] + m_delimiter + kStr[1];
                    }
                    else
                    {
                    	//서로 같거나 아이템2의 문자가 더 위에 있는 경우(문자 순서)
                    	//아이템2 아이템1 사용자 아이템2 평점 아이템1 평점으로 Value구성.
                        writeStr = kStr[0] + m_delimiter + iStr[0] + m_delimiter +
                                key.toString() + m_delimiter + kStr[1] + m_delimiter + iStr[1];
                    }
                	//아이템 i 아이템 j 사용자 아이템 i 평점 아이템 j 평점으로 Value 출력.
                    context.write(NullWritable.get(), new Text(writeStr));
                }
            }
        }
    }

    
}