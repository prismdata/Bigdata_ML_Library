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

package org.ankus.mapreduce.algorithms.recommendation.recommender;

import org.ankus.mapreduce.algorithms.recommendation.recommender.itembased.ItemSimRecommenderDriver;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * 추천 알고리즘 실행 클래스.
 * @author Wonmoon
 * @date :  2015. 1. 15
 */
public class SimilarityBasedRecommenderDriver extends Configured implements Tool {

    // SLF4J Logging
    private Logger logger = LoggerFactory.getLogger(SimilarityBasedRecommenderDriver.class);

    /**
     * main()함수로 ToolRunner를 사용하여 추천 알고리즘을 호출한다.
     * @auth 
     * @param String[] args : 추천 알고리즘 수행 인자.
     * @return
     */
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new SimilarityBasedRecommenderDriver(), args);
        System.exit(res);
    }
    
    /**
     * ToolRunner에서 호출되는 실제 알고리즘 시작 함수.
     * @auth 
     * @param String[] args : 추천 알고리즘 수행 인자.
     * @return int
     */
    @Override
    public int run(String[] args) throws Exception
    {
    	long endTime = 0;
       	long lTime  = 0;
       	long startTime = 0 ; 
       	
        Configuration conf = this.getConf();
        //conf.set("fs.default.name",  "hdfs://localhost:9000");
        if(!ConfigurationVariable.setFromArguments(args, conf))
        {
            logger.error("Job Setting Failed..");
            logger.info("Error: Job Setting Failed..: Configuration Error");
            return 1;
        }

        startTime = System.nanoTime();
        // must parameter check
        if(!checkParameters(conf))
        {
            logger.error("Configuration Error.");
            return 1;
        }

        String params[] = setParamsforSimBasedRecommendation(conf);
        
        int res = ToolRunner.run(new ItemSimRecommenderDriver(), params);
        if(res!=0) return 1;
       
        endTime = System.nanoTime();
		lTime = endTime - startTime;
		
		System.out.println("Training Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
		System.out.println("Training Finished TIME(sec):" + (lTime/1000000.0)/1000);
        return 0;
    }

    /**
     * CLI를 통해 입력 받은 인자들을 배열로 구성함.
     * @auth 
     * @param Configuration conf 하둡 환경 변수 
     * @return String[] 인자들의 배열.
     */
    private String[] setParamsforSimBasedRecommendation(Configuration conf)
    {
        String[] args = {
            ArgumentsConstants.INPUT_PATH,
            ArgumentsConstants.OUTPUT_PATH,
            ArgumentsConstants.DELIMITER,
            ArgumentsConstants.UID_INDEX,
            ArgumentsConstants.IID_INDEX,
            ArgumentsConstants.RATING_INDEX,

            ArgumentsConstants.SIMILARITY_PATH,
            ArgumentsConstants.SIMILARITY_DELIMITER,
            ArgumentsConstants.SIMILARITY_THRESHOLD,

            ArgumentsConstants.TARGET_UID,
            ArgumentsConstants.TARGET_IID_LIST,
            ArgumentsConstants.RECOMMENDATION_CNT,
            ArgumentsConstants.TEMP_DELETE
        };

        ArrayList<String> paramArr = new ArrayList<String>();

        for(String arg: args)
        {
            if(conf.get(arg, null)!=null)
            {
                paramArr.add(arg);
                paramArr.add(conf.get(arg));
            }
        }

        String params[] = new String[paramArr.size()];
        params = paramArr.toArray(params);
        return params;
    }

    /**
     * 필수 인수가 모두 기술되어 있는지 검사
     * 모두 기술되었으면 true, 아니면 false를 리턴. 
     * @author Wonmoon
     * @param Configuration conf : 하둡 환경 변수 
     * @return boolean
     */
    private boolean checkParameters(Configuration conf) throws Exception
    {
        String[] neededArgs = {
                ArgumentsConstants.INPUT_PATH,
                ArgumentsConstants.UID_INDEX,
                ArgumentsConstants.IID_INDEX,
                ArgumentsConstants.RATING_INDEX,
                ArgumentsConstants.SIMILARITY_PATH,
                ArgumentsConstants.TARGET_UID,
                ArgumentsConstants.OUTPUT_PATH
        };


        for(String arg: neededArgs)
        {
            if(conf.get(arg, null)==null)
            {
                logger.error("'" + arg + "' must be defined..");
                return false;
            }
        }

        return true;
    }
}
