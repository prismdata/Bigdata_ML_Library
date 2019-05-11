/*
*Copyright (C) 2011 ankus (http://www.openankus.org).
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

import org.ankus.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * 사용자 평점에 기반한 아이템간의 유사도를 구하는 알고리즘 <br>
 * 입력 데이터 형태 : [userID, itemID, rating]<br>
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class ItemBasedSimilarityDriver extends Configured implements Tool {

    // SLF4J Logging
    private Logger logger = LoggerFactory.getLogger(ItemBasedSimilarityDriver.class);
    public Configuration conf  = new Configuration();
    /**
     * ItemBasedSimilarity를 구동하기 위한 메인 함수.
     * @param String[] args :ItemBasedSimilarity를 구동하기 위한 인자.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new ItemBasedSimilarityDriver(), args);
        System.exit(res);
    }

    /**
    * 파라미터 설정 및 알고리즘 초기화.
    * @version 0.0.1
    * @date : 2013.07.20
    * @param String[] args:  	알고리즘 수행을 위한 사용자 인자.
    * @return 정상 종료 0, 오류 발생 1을 리턴
    */
	@Override
    public int run(String[] args) throws Exception
    {
        logger.info("Collaborative Filtering based Item Similarity Computation MR-Job is Started..");

        // configuration setting
        conf = this.getConf();
        //파라미터 초기화.
        if(!ConfigurationVariable.setFromArguments(args, conf))
        {
            logger.error("MR Job Setting Failed..");
            logger.info("Error: MR Job Setting Failed..: Configuration Error");
            return 1;
        }
        //이전 결과 삭제 
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));               
        logger.info("Output Path  '" + conf.get(ArgumentsConstants.OUTPUT_PATH) + "' will be removed ");
        
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) +"_cfSim_tmp"));               
        logger.info("Output Path  '" + conf.get(ArgumentsConstants.OUTPUT_PATH)+"_cfSim_tmp" + "' will be removed ");
            	
        long endTime = 0;
       	long lTime  = 0;
       	long startTime = 0 ; 
       	startTime = System.nanoTime();
        /**
         * INPUT_PATH, OUTPUT_PATH, DELIMITER
         * ALGORITHM_OPTION, COMMON_COUNT, UID_INDEX, IID_INDEX, RATING_INDEX, BASED_TYPE, TARGET_ID
         */
        //파라미터 검증.
        if(!checkParameters(conf))
        {
            logger.error("Configuration Error.");
            return 1;
        }
        // 1-step mr-job
        //
        String outputTmpStr = "_cfSim_tmp";
        
        /* 
         * 원본 평점 데이터 전처리 <br>
         * [사용자 아이템 평점] 의 input split을 [아이템 i, 아이템 j, 사용자, 아이템 i 평점,아이템 j 평점]으로 제구성.   
        */
        if(exec1stMRJob(conf, outputTmpStr))
        {
            logger.info("1st MR-Job is Successfully Finished...");
            /**
             * 아이템과 평점 정보를 제 구성하여 아이템 간의 상관 계수를 구해 유사도를 추정하는 MapReduce.<br>
             * Input Split key : key: offset , Value:<아이템1 아이템2 사용자 아이템1'평점 아이템2'평점><br>
             * Result data set: Key : Null, Value: <아이템1 구분자 아이템2 구분자 상관 계수(유클리드, 코사인, 피어슨)>  <br>
             */
            if(exec2ndMRJob(conf, outputTmpStr)) logger.info("2nd MR-Job is Successfully Finished...");
            else
            {
                logger.error("2nd MR-Job Failed...");
                return 1;
            }
        }
        else
        {
            logger.error("1st MR-Job Failed...");
            return 1;
        }

        // temp-delete
        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            logger.info("Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH) + outputTmpStr);
            FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputTmpStr), true);
        }
        endTime = System.nanoTime();
		lTime = endTime - startTime;
		
		System.out.println("Item Based Recommand Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
        return 0;
    }
	/**
	* 사용자가 정상적으로 인자를 설정하였는지 검증.
	* @date : 2013.07.20
    * @author Suhyun Jeon
	* @parameter Configuration conf : 하둡 환경 변수
	* @return 정상 :true, 오류 :false.
	*/		
    private boolean checkParameters(Configuration conf) throws Exception
    {
        if(conf.get(ArgumentsConstants.UID_INDEX, null)==null)
        {
            logger.error("'" + ArgumentsConstants.UID_INDEX + "' must be defined..");
            return false;
        }

        if(conf.get(ArgumentsConstants.IID_INDEX, null)==null)
        {
            logger.error("'" + ArgumentsConstants.IID_INDEX + "' must be defined..");
            return false;
        }

        if(conf.get(ArgumentsConstants.RATING_INDEX, null)==null)
        {
            logger.error("'" + ArgumentsConstants.RATING_INDEX + "' must be defined..");
            return false;
        }
        

        return true;
    }
    /**
     * 사용자 평점 데이터를 제 구성하는 MapReduce.<P>
     * [userID, itemID, rating] -> <아이템1 아이템2 사용자 아이템1-평점 아이템2-평점>.<P>
	 * @param Configuration conf : 하둡 환경 정보 
	 * @param String tmpPathStr : 출력 경로.
	 * @return 성공: true, 실폐: false
	 * @throws Exception
	 * @date : 2013.07.20
     * @author Suhyun Jeon
	 */
    public boolean exec1stMRJob(Configuration conf, String tmpPathStr) throws Exception
    {
    	Job job = new Job(this.getConf());
    	
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpPathStr));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.UID_INDEX, conf.get(ArgumentsConstants.UID_INDEX, "0"));
        job.getConfiguration().set(ArgumentsConstants.IID_INDEX, conf.get(ArgumentsConstants.IID_INDEX, "1"));
        job.getConfiguration().set(ArgumentsConstants.RATING_INDEX, conf.get(ArgumentsConstants.RATING_INDEX, "2"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_ID, conf.get(ArgumentsConstants.TARGET_ID, "-1"));

        job.setJarByClass(ItemBasedSimilarityDriver.class);

        job.setMapperClass(ItemBasedSimilarityPairMakingMapper.class);
        job.setReducerClass(ItemBasedSimilarityPairMakingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.error("Error: 1st MR for Collaborative Filtering based User/Item Similarity Computation is not Completion");
            logger.info("MR-Job is Failed..");
            return false;
        }

        return true;
    }
    /**
     * 아이템과 평점 정보를 제 구성하여 아이템 간의 상관 계수를 구해 유사도를 추정하는 MapReduce.<br>
     * Input Split key : key: offset , Value:<아이템1 아이템2 사용자 아이템1'평점 아이템2'평점><br>
     * Result data set: Key : Null, Value: <아이템1 구분자 아이템2 구분자 상관 계수(유클리드, 코사인, 피어슨)>  <br>
     * @date : 2013.07.20
     * @author Suhyun Jeon
     * @param Configuration conf : 하둡 환경 정보 
     * @param String tmpPathStr : 출력 경로.
     * @return 성공: true, 실폐: false
     * @throws Exception
     */
    public boolean exec2ndMRJob(Configuration conf, String tmpPathStr) throws Exception
    {
    	Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpPathStr);
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.ALGORITHM_OPTION, conf.get(ArgumentsConstants.ALGORITHM_OPTION, Constants.CORR_PEARSON));
        job.getConfiguration().set(ArgumentsConstants.COMMON_COUNT, conf.get(ArgumentsConstants.COMMON_COUNT, "10"));

        job.setJarByClass(ItemBasedSimilarityDriver.class);
        job.setMapperClass(ItemBasedSimilarityComputeMapper.class);
        job.setReducerClass(ItemBasedSimilarityComputeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.error("Error: 2nd MR for Collaborative Filtering based User/Item Similarity Computation is not Completion");
            logger.info("MR-Job is Failed..");
            return false;
        }
        return true;
    }
}