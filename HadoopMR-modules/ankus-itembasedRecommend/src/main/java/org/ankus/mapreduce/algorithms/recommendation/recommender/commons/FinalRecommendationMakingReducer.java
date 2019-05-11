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
 * 사용자가 구매한 아이템과 연관된 아이템들의 평균 평점을 계산.
 * @author Wonmoon
 */
public class FinalRecommendationMakingReducer extends Reducer<Text, Text, NullWritable, Text> {

    String m_delimiter;

    /**
     * 데이터 구분자를 환경 설정 변수에서 얻어온다.
     * @author Wonmoon
     * @param Context context : 하둡 환경 설정 변수
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        m_delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER);
    }
    
    /**
     * 사용자 구매 아이템의 연관 아이템과 구매 아이템의 평균 평점을 계산한다.
     * [입력] Key : 연관 아이템, Value : 사용자가 구매한 아이템의 평점 + 사용자가 구매한 아이템 아이디 + 유사도
     * [출력] Key: Null, Value : 연관 아이템 + 사용자가 구매한 아이템의 평균 평점 + 아이템 갯수
     * @author Wonmoon
     * @param Text Relative_Item : 연관된 아이템
     * @param Iterable<Text> ViewRate_View_ID_Similarity 사용자가 구매한 아이템의 평점 + 사용자가 구매한 아이템 아이디 + 유사도
     * @param Context context : 하둡 환경 설졍 변수
     * @return
     */
    @Override
    protected void reduce(Text Relative_Item, Iterable<Text> ViewRate_View_ID_Similarity, Context context) throws IOException, InterruptedException
    {
        Iterator<Text> iter = ViewRate_View_ID_Similarity.iterator();

        int cnt = 0;
        double recomVal = 0.0;
        while (iter.hasNext())
        {
            cnt++;
            String tokens[] = iter.next().toString().split(m_delimiter);
            recomVal += Double.parseDouble(tokens[0]);
        }
        
        recomVal = recomVal / (double)cnt;
        double point_ = 1000d;
        recomVal = Math.round(recomVal * point_)/ point_;
        
        String valueStr = Relative_Item.toString() + m_delimiter + recomVal + m_delimiter + cnt;
        context.write(NullWritable.get(), new Text(valueStr));
    }

    /**
     * 추천 대상 아이템을 지정할 경우  출력 폴더에 있는 모든 파일을 1개의 파일로 병합.
     * 지정되지 않은 경우 출력 폴더에 있는 내용을 객체로 읽은 후 정렬 하여 파일로 병합.
     * @author Wonmoon
     * @param Configuration conf : 하둡 환경 변수
     * @param String readPath : FinalRecommendationMakingReducer 출력 경로
     * @param String outputFile : 병합 결과가 저장될 경로.
     * @return void
     */
    public static void finalRecomResultWriting(Configuration conf, String readPath, String outputFile) throws Exception
    {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(readPath));
        FSDataOutputStream fout = fs.create(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputFile), true);
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
            String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
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
            int recomCnt = Integer.parseInt(conf.get(ArgumentsConstants.RECOMMENDATION_CNT, "10"));
            if(resultList.size() < recomCnt) recomCnt = resultList.size();

            for(int i=0; i<recomCnt; i++) bw.write(resultList.get(i).toString(delimiter) + "\n");
        }

        bw.close();
        fout.close();

    }
}
