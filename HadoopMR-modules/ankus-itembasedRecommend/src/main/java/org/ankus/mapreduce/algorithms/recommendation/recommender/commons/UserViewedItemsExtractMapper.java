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

package org.ankus.mapreduce.algorithms.recommendation.recommender.commons;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * 사용자가 평가한  아이템 정보만 추출 클래스
 * @author Wonmoon
 */
public class UserViewedItemsExtractMapper extends Mapper<Object, Text, NullWritable, Text>{

    String m_delimiter;
    String m_targetUID;
    int m_uidIndex;
    int m_iidIndex;
    int m_ratingIndex;
	 
    /**
	  * Mapper 초기 설정 함수
	  * 사용자, 아이템, 평점의 인덱스를 획득하고 사용자가 지정한 아이템을 획득한다.
	  * @author Wonmoon
	  * @param Context context : 하둡 환경 설정 변수
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
    }

    /**
     * 전체 데이터에서 사용자가 지정한 아이템의 평가 정보를 로드한다.
     * [입력] 사용자 아이템 평점 레코드
     * [출력] 사용자, 지정한 아이템, 평점
     * @author Wonmoon
     * @param Object key 데이터 오프셋
     * @param Text value 아이템 평가 레코드
     * @param Context context : 하둡 환경 설정 변수
     */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
        String tokens[] = value.toString().split(m_delimiter);
        if(tokens[m_uidIndex].equals(m_targetUID))
        {
            String valueStr = tokens[m_uidIndex] + m_delimiter + tokens[m_iidIndex] + m_delimiter + tokens[m_ratingIndex];
            context.write(NullWritable.get(), new Text(valueStr));
        }
	}

	/**
	 * 사용자가 구매한 아이템의 목록을 구분자로 구분하여1줄로 만들고 리턴함.
	 * @author Wonmoon
	 * @param Configuration conf : 하둡 환경 설정 변수
	 * @param String readPath : 구매 아이템 저장 경로
	 * @return String : 1줄의 아이템 목록
	 */
    public static String getUserViewListString(Configuration conf, String readPath) throws Exception
    {
        String confSetStr = null;

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(readPath));

        int similUserCnt = 0;
        for (int i=0;i<status.length;i++)
        {
            if(!status[i].getPath().toString().contains("part-")) continue;

            FSDataInputStream fin = fs.open(status[i].getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

            String readStr = "";
            while((readStr = br.readLine())!=null)
            {
                if(confSetStr!=null) confSetStr += conf.get(ArgumentsConstants.DELIMITER) + readStr;
                else confSetStr = readStr;

                similUserCnt++;
            }

            br.close();
            fin.close();
        }

        return confSetStr;
    }

}