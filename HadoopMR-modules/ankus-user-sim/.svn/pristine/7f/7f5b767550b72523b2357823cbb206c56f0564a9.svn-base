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
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 동일 사용자 쌍에 대해 공유한 아이템의 점수 쌍을 취합하여 유사도 계산.
 * CFBasedSimilarityComputeReducer
 * @desc
 *      User-based Collaborative Filtering recommendation algorithms
 *      1. Cosine coefficient 2. Pearson coefficient
 *      Required data set : [userID, itemID, rating]
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class CFBasedSimilarityComputeReducer extends Reducer<Text, Text, NullWritable, Text> {

    private String m_delimiter;
    private String m_algorithmOption;
    private int m_commonCount;
    private double m_corrValLimit = 0.0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER);
        m_algorithmOption = conf.get(ArgumentsConstants.ALGORITHM_OPTION);
        m_commonCount = Integer.parseInt(conf.get(ArgumentsConstants.COMMON_COUNT));
    }
/**
*사용자간 평점을 기반으로 유사도를 계산함. 
*@param input key: user1@@user2 value: rate1\rate2
* <br>emit user1@@user2@@유사도.
*@return void
 */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        Iterator<Text> iter = values.iterator();

        double corrVal = 0.0;
        double point_ = 1000d;
        
        if(m_algorithmOption.equals(Constants.CORR_UCLIDEAN)) corrVal = getUclideanCorr(iter, m_commonCount);
        else if(m_algorithmOption.equals(Constants.CORR_COSINE)) corrVal = getCosineCorr(iter, m_commonCount);
        else if(m_algorithmOption.equals(Constants.CORR_PEARSON)) corrVal = getPearsonCorr(iter, m_commonCount);
        
        corrVal = Math.round(corrVal * point_) / point_;
        
        if(Math.abs(corrVal) > m_corrValLimit)
        {
            String writeStr = key.toString().replaceAll("@@", m_delimiter);
            writeStr += m_delimiter + corrVal;
            context.write(NullWritable.get(), new Text(writeStr));
        }
    }
    /**
    *사용자간 평점을 유클리디안 거리를 이용하여 유사도 계산.
     */
    private double getUclideanCorr(Iterator<Text> iter, int commonCnt)
    {
        int cnt = 0;
        double retVal = 0.0;
        double minus2Sum = 0.0;

        while (iter.hasNext())
        {
            cnt++;
            String tokens[] = iter.next().toString().split(m_delimiter);

            double x = Double.parseDouble(tokens[0]);
            double y = Double.parseDouble(tokens[1]);

            minus2Sum += Math.pow((x-y), 2);
        }
        retVal = Math.sqrt(minus2Sum);

        if(cnt >= commonCnt) return retVal;
        else return 0;
    }
    /**
     *사용자간 평점을 코사인 함수를 이용하여 유사도 계산.
      */
    private double getCosineCorr(Iterator<Text> iter, int commonCnt)
    {
        int cnt = 0;
        double retVal = 0.0;

        double x2Sum = 0.0;
        double y2Sum = 0.0;
        double xySum = 0.0;

        while (iter.hasNext())
        {
            cnt++;
            String tokens[] = iter.next().toString().split(m_delimiter);

            double x = Double.parseDouble(tokens[0]);
            double y = Double.parseDouble(tokens[1]);

            x2Sum += Math.pow(x,2);
            y2Sum += Math.pow(y,2);
            xySum += (x*y);
        }
        retVal = xySum / (Math.sqrt(x2Sum) * Math.sqrt(y2Sum));

        if(cnt >= commonCnt) return retVal;
        else return 0;
    }
    /**
     *사용자간 평점을 피어슨 상관계수를 이용하여 유사도 계산.
      */
    private double getPearsonCorr(Iterator<Text> iter, int commonCnt)
    {
        int cnt = 0;
        double retVal = 0.0;

        double xSum = 0.0;
        double ySum = 0.0;
        double x2Sum = 0.0;
        double y2Sum = 0.0;
        double xySum = 0.0;

        while (iter.hasNext())
        {
            cnt++;
            String tokens[] = iter.next().toString().split(m_delimiter);

            double x = Double.parseDouble(tokens[0]);
            double y = Double.parseDouble(tokens[1]);

            xSum += x;
            ySum += y;
            x2Sum += Math.pow(x, 2);
            y2Sum += Math.pow(y, 2);
            xySum += (x*y);
        }
        retVal = ((cnt * xySum) - (xSum * ySum))
                / ((Math.sqrt((cnt * x2Sum) - Math.pow(xSum,2))) * (Math.sqrt((cnt * y2Sum) - Math.pow(ySum,2))));

        if(cnt >= commonCnt) return retVal;
        else return 0;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
