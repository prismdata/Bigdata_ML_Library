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
 * 사용자 기반 협업적 필터링 기반 유사도 계산 모듈.
 * CFBasedSimilarityDriver
 *     User-based Collaborative Filtering recommendation algorithms
 *      1. Cosine coefficient 2. Pearson coefficient
 *      Required data set : [userID, itemID, rating]
 * @version 0.0.1
 * @date : 2013.07.20
 * @author Suhyun Jeon
 */
public class UserBasedSimilarityDriver extends Configured implements Tool {

    // SLF4J Logging
    private Logger logger = LoggerFactory.getLogger(UserBasedSimilarityDriver.class);
    /**
     * 메인 함수 
     * @param 실행 파라이터.(내부 실행용)
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new UserBasedSimilarityDriver(), args);
        System.exit(res);
    }
 /**
  * GenericOptionsParser와 함께 작동하여 generic hadoop 명령 줄 인수를 구문 분석.<br>
  * modifies the Configuration of the Tool. 
  * @param 실행 파라이터.(내부 실행용)
  * @throws Exception
  */
	@Override
    public int run(String[] args) throws Exception
    {
        logger.info("Collaborative Filtering based User/Item Similarity Computation MR-Job is Started..");
        //하둡 환경 정보 로드.
        Configuration conf = this.getConf();
        if(!ConfigurationVariable.setFromArguments(args, conf))
        {
            logger.error("MR Job Setting Failed..");
            logger.info("Error: MR Job Setting Failed..: Configuration Error");
            return 1;
        }
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

        // must parameter check
        if(!checkParameters(conf))
        {
            logger.error("Configuration Error.");
            return 1;
        }

        // 2-step mr-job
        String outputTmpStr = "_cfSim_tmp";
        if(exec1stMRJob(conf, outputTmpStr))
        {
            logger.info("1st MR-Job is Successfully Finished...");
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
		
		System.out.println("User Based Recommand Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
        return 0;
    }

	/**
	 * 파라미터 검사.
	 * @param conf
	 * @return boolean.
	 * @throws Exception
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
        conf.set(ArgumentsConstants.BASED_TYPE, "user");
        return true;
    }
/**
 * 아이템, 사용자, 평점 정보 구성.
 * @param conf
 * @param tmpPathStr
 * @return boolean
 * @throws Exception
 */
    private boolean exec1stMRJob(Configuration conf, String tmpPathStr) throws Exception
    {
    	Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpPathStr));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.UID_INDEX, conf.get(ArgumentsConstants.UID_INDEX, "0"));
        job.getConfiguration().set(ArgumentsConstants.IID_INDEX, conf.get(ArgumentsConstants.IID_INDEX, "1"));
        job.getConfiguration().set(ArgumentsConstants.RATING_INDEX, conf.get(ArgumentsConstants.RATING_INDEX, "2"));
        job.getConfiguration().set(ArgumentsConstants.BASED_TYPE, conf.get(ArgumentsConstants.BASED_TYPE, Constants.RECOM_USER_BASED));
        job.getConfiguration().set(ArgumentsConstants.TARGET_ID, conf.get(ArgumentsConstants.TARGET_ID, "-1"));

        job.setJarByClass(UserBasedSimilarityDriver.class);

        job.setMapperClass(CFBasedSimilarityPairMakingMapper.class);
        job.setReducerClass(CFBasedSimilarityPairMakingReducer.class);

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
 * 동일 사용자 쌍에 대해 아이템의 점수 쌍을 이용하여 유사도를 계산.
 * input (user1, user2)\t (rate1, rate2)
 * output (user1, user2) 유사도 
 * @param conf
 * @return boolean
 * @throws Exception
 * */
    private boolean exec2ndMRJob(Configuration conf, String tmpPathStr) throws Exception
    {
    	Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpPathStr);
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.ALGORITHM_OPTION, conf.get(ArgumentsConstants.ALGORITHM_OPTION, Constants.CORR_PEARSON));
        job.getConfiguration().set(ArgumentsConstants.COMMON_COUNT, conf.get(ArgumentsConstants.COMMON_COUNT, "10"));

        job.setJarByClass(UserBasedSimilarityDriver.class);

        job.setMapperClass(CFBasedSimilarityComputeMapper.class);
        job.setReducerClass(CFBasedSimilarityComputeReducer.class);

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