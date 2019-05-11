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


import org.ankus.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 알고리즘 실제 수행 클래스.
 * @author Wonmoon
 * @date :  2015. 1. 15
 */
public class ContentBasedSimilarityDriver extends Configured implements Tool {

    // SLF4J Logging
    private Logger logger = LoggerFactory.getLogger(ContentBasedSimilarityDriver.class);
    
    /**
     * main()함수로 ToolRunner를 사용하여 컨텐츠 기반 유사도를 호출한다.
     * @auth 
     * @parameter String[] args : 유사도 분석 알고리즘 수행 인자.
     * @return
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ContentBasedSimilarityDriver(), args);
        System.exit(res);
    }
    
    /**
     * ToolRunner에서 호출되는 실제 알고리즘 시작 함수.
     * @auth Wonmoon
     * @parameter String[] args : 유사도 분석 알고리즘 수행 인자.
     * @return int
     */
    public int run(String[] args) throws Exception
    {
    	long endTime = 0;
	   	long lTime  = 0;
	   	long startTime = 0 ; 
	   	
        logger.info("Contents based Item Similarity Computation MR-Job is Started..");

        // configuration setting
        Configuration conf = this.getConf();
        //conf.set("fs.default.name",  "hdfs://localhost:9000");
        if(!ConfigurationVariable.setFromArguments(args, conf))
        {
            logger.error("MR Job Setting Failed..");
            logger.info("Error: MR Job Setting Failed..: Configuration Error");
            return 1;
        }
        
        startTime = System.currentTimeMillis();
        // 2-step mr-job
                 
        /*
         * [입력] 아이템 아이디 구분자1 아이템 명칭 구분자1, <속성1 구분자2 속성2 구분자2 속성3....>
         * [출력] 아이템 아이디1 + 아이템 아이디2 + 속성 번호 + 유사도 +  1
         */
        String outputTmpStr = "_cbSim_tmp";
        if(exec1stMRJob(conf, outputTmpStr))
        {
            logger.info("1st MR-Job is Successfully Finished...");
            /*
             * [입력]  아이템 아이디1 + 아이템 아이디2 + 속성 번호 + 유사도 +  1
             * [출력]  아이템1 + 아이템2 + 유사도 + 유사도 계산에 사용된 속성의 종류 수
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
        
        endTime = System.currentTimeMillis();
		lTime = endTime - startTime;
		System.out.println("Correlation Calculation Processing Time : "+ lTime/1000.0f +"초");
		
        logger.info("Contents based Item Similarity Computation MR-Job is Finished..");
        return 0;
    }
  
    /**
     * 다수의 문자열로 이루어진 개별 속성에 대하여 Jaccard, Dice알고리즘을 사용하여 유사도를 계산한다.
     * [입력] 아이템 아이디 구분자1 아이템 명칭 구분자1, 속성1 구분자2 속성2 구분자2 속성3....
     * [출력] Key: Null,  Value : 아이템 아이디1 + 아이템 아이디2 + 속성 번호 + 유사도 +  1
     * @auth Suhyun Jeon
     * @parameter Configuration conf : 하둡 환경 설정 변수
     * @parameter String tmpPathStr : 유사도 출력 경로
     * @return
     */
    private boolean exec1stMRJob(Configuration conf, String tmpPathStr) throws Exception
    {
    	Job job = new Job(this.getConf());
    	
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpPathStr));
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpPathStr));
        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.SUB_DELIMITER, conf.get(ArgumentsConstants.SUB_DELIMITER, ","));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.KEY_INDEX, conf.get(ArgumentsConstants.KEY_INDEX, "0"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_ID, conf.get(ArgumentsConstants.TARGET_ID, "-1"));
        job.getConfiguration().set(ArgumentsConstants.ALGORITHM_OPTION, conf.get(ArgumentsConstants.ALGORITHM_OPTION, Constants.CORR_JACCARD));

        job.setJarByClass(ContentBasedSimilarityDriver.class);

        job.setMapperClass(ContentBasedSimilarityAttrSimMapper.class);
        job.setReducerClass(ContentBasedSimilarityAttrSimReducer.class);

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
     * [입력]  아이템 아이디1 + 아이템 아이디2 + 속성 번호 + 유사도 +  1
     * [출력]  Key : Null, Value : 아이템1 + 아이템2 + 유사도 + 유사도 계산에 사용된 속성 수.
     * @auth
     * @parameter Configuration conf : 하둡 환경 설정 변수
     * @parameter String tmpPathStr : 아이템간 속성의 평균 유사도(jaccard/dice)
     * @return
     */
    private boolean exec2ndMRJob(Configuration conf, String tmpPathStr) throws Exception
    {
    	Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpPathStr);
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
        
        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.SUMMATION_OPTION, conf.get(ArgumentsConstants.SUMMATION_OPTION, Constants.RECOM_CB_AVGSUM));

        job.setJarByClass(ContentBasedSimilarityDriver.class);

        job.setMapperClass(ContentBasedSimilaritySimSumMapper.class);
        job.setReducerClass(ContentBasedSimilaritySimSumReducer.class);

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