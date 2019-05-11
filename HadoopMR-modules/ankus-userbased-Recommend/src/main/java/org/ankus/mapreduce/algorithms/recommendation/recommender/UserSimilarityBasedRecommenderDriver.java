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

import org.ankus.mapreduce.algorithms.recommendation.recommender.userbased.UserSimRecommenderDriver;
import org.ankus.util.Constants;
import org.ankus.util.ConfigurationVariable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 *사용자 유사도 기반 추천 알고리즘  모듈.
 * User: Wonmoon
 * Date: 15. 1. 15
 * Time: 오후 5:47
 * To change this template use File | Settings | File Templates.
 */
public class UserSimilarityBasedRecommenderDriver extends Configured implements Tool {

    // SLF4J Logging
    private Logger logger = LoggerFactory.getLogger(UserSimilarityBasedRecommenderDriver.class);

    /**
     * 드라이버를 호출하기 위한 메인 함수.
     * @param args : 알고리즘 수행 파라미터.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new UserSimilarityBasedRecommenderDriver(), args);
        System.exit(res);
    }
    long endTime = 0;
   	long lTime  = 0;
   	long startTime = 0 ; 
   	/**
   	 * GenericOptionsParser와 함께 작동하여 generic hadoop 명령 줄 인수를 구문 분석.<br>
	*It works in conjunction with GenericOptionsParser to parse the generic hadoop command line arguments and modifies the Configuration of the Tool.
   	 */
    @Override
    public int run(String[] args) throws Exception
    {

    	Configuration conf = this.getConf();
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
        FileSystem.get(conf).delete(new Path(conf.get(Constants.OUTPUT_PATH)));//FOR LOCAL TEST                   
    	logger.info("Output Path  '" + conf.get(Constants.OUTPUT_PATH) + "' will be removed ");

    	
    	FileSystem.get(conf).delete(new Path(conf.get(Constants.OUTPUT_PATH)+"_similUsers"));//FOR LOCAL TEST                   
    	logger.info("Output Path  '" + conf.get(Constants.OUTPUT_PATH) +"_similUsers"+ "' will be removed ");
    	
    	FileSystem.get(conf).delete(new Path(conf.get(Constants.OUTPUT_PATH)+"_finalReduce"));//FOR LOCAL TEST                   
    	logger.info("Output Path  '" + conf.get(Constants.OUTPUT_PATH) +"_finalReduce"+ "' will be removed ");
    	
        String params[] = setParamsforSimBasedRecommendation(conf);
        int res = ToolRunner.run(new UserSimRecommenderDriver(), params);
        if(res!=0) return 1;
        
        endTime = System.nanoTime();
		lTime = endTime - startTime;
		
		System.out.println("Training Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
		System.out.println("Training Finished TIME(sec):" + (lTime/1000000.0)/1000);
        return 0;
    }
    
    /**
     * 사용자 유사도 기반 추천 알고리즘 수행을 위한 파라미터 구성.
     * @param Configuration conf : 하둡 환경 변수.
     * @return boolean
     */
    private String[] setParamsforSimBasedRecommendation(Configuration conf)
    {
        String[] args = {
                Constants.INPUT_PATH,
                Constants.DELIMITER,
                Constants.UID_INDEX,
                Constants.IID_INDEX,
                Constants.RATING_INDEX,

                Constants.SIMILARITY_PATH,
                Constants.BASED_TYPE,
                Constants.SIMILARITY_DELIMITER,
                Constants.SIMILARITY_THRESHOLD,

                Constants.TARGET_UID,
                Constants.TARGET_IID_LIST,
                Constants.RECOMMENDATION_CNT,

                Constants.OUTPUT_PATH
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
     * 알고리즘 수행 변수 체크 함수.
     * @param Configuration conf
     * @return boolean
     * @throws Exception
     */
    private boolean checkParameters(Configuration conf) throws Exception
    {
        String[] neededArgs = {
                Constants.INPUT_PATH,
                Constants.UID_INDEX,
                Constants.IID_INDEX,
                Constants.RATING_INDEX,
                Constants.SIMILARITY_PATH,
                Constants.BASED_TYPE,
                Constants.TARGET_UID,
                Constants.OUTPUT_PATH
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
