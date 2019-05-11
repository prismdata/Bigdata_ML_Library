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
 * 대상 사용자가 이미 평가한 아이템만 추출하는 Mapper클래스.
 * @author Wonmoon
 *
 */
public class UserViewedItemsExtractMapper extends Mapper<Object, Text, NullWritable, Text>{

    String m_delimiter;
    String m_targetUID;
    int m_uidIndex;
    int m_iidIndex;
    int m_ratingIndex;

    /**
     * Mapper 실행 환경  설정.
     *<br>각 Mapper에서 1번만 실행됨.
     *
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        m_delimiter = conf.get(Constants.DELIMITER);
        m_targetUID = conf.get(Constants.TARGET_UID);
        m_uidIndex = Integer.parseInt(conf.get(Constants.UID_INDEX));
        m_iidIndex = Integer.parseInt(conf.get(Constants.IID_INDEX));
        m_ratingIndex = Integer.parseInt(conf.get(Constants.RATING_INDEX));
    }

    /**
     * 사용자 평점 데이터로부터 추천 대상 사용자의 아이템 평점, 정보 획득.
     * @param value=사용자 ID, 아이템 ID, 평점
     * emit : 사용자 ID, 아이템 ID, 평점
     * @return void
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
	 * extractUserViewedItems_MapLoad의 결과를 한 줄로 결합.
	 * @param conf
	 * @param readPath extractUserViewedItems_MapLoad 출력 경로.
	 * @return
	 * @throws Exception
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
                if(confSetStr!=null) confSetStr += conf.get(Constants.DELIMITER) + readStr;
                else confSetStr = readStr;

                similUserCnt++;
            }

            br.close();
            fin.close();
        }

        return confSetStr;
    }

}
