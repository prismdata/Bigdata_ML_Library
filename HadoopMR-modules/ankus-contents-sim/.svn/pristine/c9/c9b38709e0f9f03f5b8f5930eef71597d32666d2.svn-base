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

package org.ankus.mapreduce.algorithms.recommendation.similarity.contentbased;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * 두개의 비교 대상의 컨텐츠 속성들에 대해 jarccard/dice 알고리즘을 이용하여 유사도를 측정한다.
 * @version 0.1
 * @date : 2013.09.25
 * @author Suhyun Jeon
*/
public class ContentBasedSimilarityAttrSimReducer extends Reducer<Text, Text, NullWritable, Text> {

    private String m_delimiter;
    private String m_subDelimiter;
    private String m_targetID;
    private String m_algoOpt;
    private Logger logger = LoggerFactory.getLogger(ContentBasedSimilarityAttrSimReducer.class);
    /**
     * reduce함수를 수행하기 위해 아이템 아이디 구분자1, 컨텐츠 속성 구분자2, 비교 대상 아이디, 유사도 알고리즘 명을 설정한다. 
     * @auth Suhyun Jeon
     * @parameter Context context :하둡 환경 설정 변수
     * @return
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        //아이템 아이디와 컨텐츠 속성을 구분하는 문자열
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER);
        
        //컨텐츠 속성들을 구분하는 문자열
        m_subDelimiter = conf.get(ArgumentsConstants.SUB_DELIMITER);
        m_targetID = conf.get(ArgumentsConstants.TARGET_ID);
        m_algoOpt = conf.get(ArgumentsConstants.ALGORITHM_OPTION);
    }

    /**
     *  두개의 비교 대상의 컨텐츠 속성들에 대해 jarccard/dice 알고리즘을 이용하여 유사도를 측정한다.
     *  [입력] Key: attr-인덱스 Value: 컨텐츠 분류명1+구분자2+컨텐츠 분류명2,..
     *  [출력] Key: Null Value : 아이템 아이디1 + 아이템 아이디2 + 속성 번호 + 유사도 +  1
     * @auth Suhyun Jeon
     * @parameter Text key
     * @parameter Iterable<Text> values
     * @parameter Context context
     * @return
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
    	ArrayList<String> valueList = new ArrayList<String>();
    	
        Iterator<Text> iter = values.iterator();
        while (iter.hasNext()) {
        	valueList.add(iter.next().toString());
        }

        for(int i=0; i<valueList.size(); i++)
        {
            String iStr[] = valueList.get(i).split(m_delimiter); //ITEM ID, ITEM ATTRIBUTE로 분리.
            
            for(int k=i+1; k<valueList.size(); k++) 
            {
                String kStr[] = valueList.get(k).split(m_delimiter);//ITEM ID, ITEM ATTRIBUTE로 분리.
                boolean isWrite = true;
                
                String src_id = iStr[0];
                String target_id = kStr[0];                
                if(!m_targetID.equals("-1"))
                {
                    if(!src_id.equals(m_targetID) && !target_id.equals(m_targetID)) isWrite = false;
                }

                if(isWrite)
                {
                	String src_attribute = iStr[1];
                	String target_attribute = kStr[1];     
                	
                    double corrVal = getCorr(src_attribute, target_attribute, m_subDelimiter, m_algoOpt);                    
                    String writeStr = "";                    
                    if(src_id.compareTo(target_id) < 0)
                    {
                        writeStr = src_id + m_delimiter + target_id + m_delimiter +  key.toString() + m_delimiter + corrVal + m_delimiter + "1";
                    }
                    else
                    {
                        writeStr = target_id + m_delimiter + src_id+ m_delimiter +  key.toString() + m_delimiter + corrVal + m_delimiter + "1";
                    }
                    
                    context.write(NullWritable.get(), new Text(writeStr));
                }
            }
        }
    }

    /**
     * 특정 구분자로 이루어진 두개의 문자열 백터에 해새 jaccard, dice방법으로 유사도를 구한다.
     * @auth
     * @parameter
     * @return
     */
    private double getCorr(String str1, String str2, String delimiter, String algoOpt)
    {
        double retCorr = 0.0;
        String str1Arr[] = str1.split(delimiter);
        String str2Arr[] = str2.split(delimiter);

        double xANDy = 0.0;
        for(String s1: str1Arr)
        {
            for(String s2: str2Arr) if(s1.equals(s2)) xANDy++;
        }

        if(xANDy == 0) retCorr = 0.0;
        else
        {
            if(algoOpt.equals(Constants.CORR_DICE)) retCorr = (2.0 * xANDy) / ((double)str1Arr.length + (double)str2Arr.length);
            else if(algoOpt.equals(Constants.CORR_JACCARD)) retCorr = xANDy / ((double)str1Arr.length + (double)str2Arr.length - xANDy);
        }

        return retCorr;
    }

}