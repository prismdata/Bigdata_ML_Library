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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/**
 * 추천 결과를 출력하는 클래스.
 */
public class FinalRecommendationMakingReducer extends Reducer<Text, Text, NullWritable, Text> {

    String m_delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        m_delimiter = context.getConfiguration().get(Constants.DELIMITER);
    }

    /**
     *키 아이템에 대한 유사한 사용자의 평균 평점을 출력함.
     * @param key: 아이템, value :평점+사용자+유사 사용자에 대한 유사도.
     * emit : 아이템 ID,  평균 평점(추천 점수), 유사한 사용자 수.
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        Iterator<Text> iter = values.iterator();

        int cnt = 0;
        double recomVal = 0.0;
        double point_ = 1000d;
//        동일한 아이템에 대한 평점을 합하고 그 평균을 산출한다.
        while (iter.hasNext())
        {
            cnt++;
            String tokens[] = iter.next().toString().split(m_delimiter);
            recomVal += Double.parseDouble(tokens[0]);
        }
        recomVal = recomVal / (double)cnt;
        recomVal = Math.round(recomVal * point_) / point_;
        
        String valueStr = key.toString() + m_delimiter + recomVal + m_delimiter + cnt;
        context.write(NullWritable.get(), new Text(valueStr));
    }


    /**
     * 추천 결과를 받아서  상위에서 -recomCnt 만큼만 파일로 출력한다.
     * @param conf 하둡 환경 변수.
     * @param readPath 아이템 ID,  평균 평점(추천 점수), 유사한 사용자 수. 
     * @param outputFile
     * @throws Exception
     */
    public static void finalRecomResultWriting(Configuration conf, String readPath, String outputFile) throws Exception
    {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(readPath));
        FSDataOutputStream fout = fs.create(new Path(conf.get(Constants.OUTPUT_PATH) + outputFile), true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));

        if(conf.get(Constants.RECOMJOB_ITEM_DEFINED).equals("true"))
        {
            for(int i=0; i<status.length; i++)
            {
                if(!status[i].getPath().toString().contains("part-")) continue;

                FSDataInputStream fin = FileSystem.get(conf).open(status[i].getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

                String readStr;
                while((readStr = br.readLine())!=null) bw.write(readStr + "\n");

                br.close();
                fin.close();
            }
        }
        else
        {
            String delimiter = conf.get(Constants.DELIMITER, "\t");
            ArrayList<RecomResultStructure> resultList = new ArrayList<RecomResultStructure>();

            for(int i=0; i<status.length; i++)
            {
                if(!status[i].getPath().toString().contains("part-")) continue;

                FSDataInputStream fin = FileSystem.get(conf).open(status[i].getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

                String readStr, tokens[];
                while((readStr = br.readLine())!=null)
                {
                    tokens = readStr.split(delimiter);
                    RecomResultStructure recom = new RecomResultStructure(tokens[0], Double.parseDouble(tokens[1]), Integer.parseInt(tokens[2]));
                    resultList.add(recom);
                }

                br.close();
                fin.close();
            }

            System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
            Collections.sort(resultList, new Comparator<RecomResultStructure>() {
                @Override
                public int compare(RecomResultStructure o1, RecomResultStructure o2) {

                    double value = o1.getRecomValue() - o2.getRecomValue();

                    // Decending order
                    if (value > 0) return -1;
                    else if (value < 0) return 1;
                    else return 0;
                }
            });
            int recomCnt = Integer.parseInt(conf.get(Constants.RECOMMENDATION_CNT, "10"));
            if(resultList.size() < recomCnt) recomCnt = resultList.size();

            for(int i=0; i<recomCnt; i++) bw.write(resultList.get(i).toString(delimiter) + "\n");
        }

        bw.close();
        fout.close();

    }
}