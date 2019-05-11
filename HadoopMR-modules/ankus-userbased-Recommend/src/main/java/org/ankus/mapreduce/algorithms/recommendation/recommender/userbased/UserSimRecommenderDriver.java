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

package org.ankus.mapreduce.algorithms.recommendation.recommender.userbased;

import org.ankus.mapreduce.algorithms.recommendation.recommender.commons.FinalRecommendationMakingReducer;
import org.ankus.mapreduce.algorithms.recommendation.recommender.commons.UserViewedItemsExtractMapper;
import org.ankus.util.Constants;
import org.ankus.util.ConfigurationVariable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 *사용자간의 유사도를 이용하여 특정 사용자에 대한 추천 결과를 생성하는 분석 모듈.
 * @author: Wonmoon
 * Date: 15. 1. 14
 * Time: 오후 3:44
 * To change this template use File | Settings | File Templates.
 */
public class UserSimRecommenderDriver extends Configured implements Tool {

    // SLF4J Logging
    private Logger logger = LoggerFactory.getLogger(UserSimRecommenderDriver.class);
    long endTime = 0;
   	long lTime  = 0;
   	long startTime = 0 ; 
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new UserSimRecommenderDriver(), args);
        System.exit(res);
    }
    /**
     * GenericOptionsParser와 함께 작동하여 generic hadoop 명령 줄 인수를 구문 분석.<br>
     * It works in conjunction with GenericOptionsParser to parse the generic hadoop command line arguments and 
     * modifies the Configuration of the Tool. 
     * @param 실행 파라이터.
      * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception
    {
        logger.info("User Similarity/Correlation based Recommendation is Started..");

        // configuration setting
        Configuration conf = this.getConf();
//        conf.set("fs.default.name",  "hdfs://localhost:9000");
        if(!ConfigurationVariable.setFromArguments(args, conf))
        {
            logger.error("Job Setting Failed..");
            logger.info("Error: Job Setting Failed..: Configuration Error");
            return 1;
        }

        startTime = System.nanoTime();
        // process and mr-job
        boolean isOK = true;
        String tmpOutput_simUsers = "_similUsers";
        String tmpOutput_userViews = "_userViews";
        String tmpOutput_finalReduce = "_finalReduce";
    	startTime = System.nanoTime();
    	
//    	목표 사용자 및 유사도 정보 임계값을 만족하는 정보 필터링
        if(!extractSimilUsers_MapLoad(conf, tmpOutput_simUsers)) isOK = false;
        else
        {
        	//추천할 아이템 아이디가 없는 경우.
            if(conf.get(Constants.TARGET_IID_LIST, null)==null)
            {
                conf.set(Constants.RECOMJOB_ITEM_DEFINED, "false");      
                //추천 대상에 대한 사용자+아이템+평점을 필터링하고 한줄로 생성.
                //user_viwed_list에 저장(설정함.)
                if(!extractUserViewedItems_MapLoad(conf, tmpOutput_userViews)) isOK = false;
            }
            else conf.set(Constants.RECOMJOB_ITEM_DEFINED, "true");
            
            //추천대상자, (추천 아이템),  유사도 정보를 이용하여 아이템을 추천함.
            if(isOK && !makingRecoms_MapReduce(conf, tmpOutput_finalReduce)) isOK = false;
        }

        FileSystem.get(conf).delete(new Path(conf.get(Constants.OUTPUT_PATH) + tmpOutput_simUsers), true);
        FileSystem.get(conf).delete(new Path(conf.get(Constants.OUTPUT_PATH) + tmpOutput_userViews), true);
        FileSystem.get(conf).delete(new Path(conf.get(Constants.OUTPUT_PATH) + tmpOutput_finalReduce), true);

        endTime = System.nanoTime();
		lTime = endTime - startTime;
		
		System.out.println("Training Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
		System.out.println("Training Finished TIME(sec):" + (lTime/1000000.0)/1000);
        if(isOK)
        {
            logger.info("User Similarity/Correlation based Recommendation is Finished..");
            return 0;
        }
        else return 1;
    }
/**
 * 목표 사용자와 유사도 임계값을 만족하는 사용자만 획득하는 Job을 수행(Mapper만 수행)
 * @param Configuration conf. String temporary output path
 * @return boolean
 * @throws Exception
 */
    private boolean extractSimilUsers_MapLoad(Configuration conf, String tmpOutputStr) throws Exception
    {
    	Job job = new Job(this.getConf());

        String outputPath = conf.get(Constants.OUTPUT_PATH) + tmpOutputStr;
        FileInputFormat.addInputPaths(job, conf.get(Constants.SIMILARITY_PATH));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().set(Constants.DELIMITER, conf.get(Constants.DELIMITER, "\t"));
        job.getConfiguration().set(Constants.SIMILARITY_DELIMITER, conf.get(Constants.SIMILARITY_DELIMITER, "\t"));
        job.getConfiguration().set(Constants.TARGET_UID, conf.get(Constants.TARGET_UID, null));
        job.getConfiguration().set(Constants.SIMILARITY_THRESHOLD, conf.get(Constants.SIMILARITY_THRESHOLD, "0.8"));

        job.setJarByClass(UserSimRecommenderDriver.class);
        job.setMapperClass(SimilUserExtractMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.error("Error: Map Job for Similar User's Information Extraction is Failed...");
            logger.info("MR-Job is Failed..");
            return false;
        }

        // extracted results > configuration loading
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(outputPath));

        String confSetStr = null;
        int similUserCnt = 0;
        
        //필터링된 추천 대상 사용자 정보 파일 목록 검색하여 유사 사용자 목록을 획득함. 
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
        //유사 사용자 정보를 configuration에 저장함.
        if(confSetStr!=null) conf.set(Constants.RECOMJOB_SIMIL_USER_INFOS, confSetStr);
        else
        {
            logger.info("There is no Similar Users that have value " + conf.get(Constants.SIMILARITY_THRESHOLD, "0.8") + " over. Recommendation can not be executed more..");
            return false;
        }

        return true;
    }

/**
 * 사용자 평점 데이터로부터 추천을 원하는 평점 자료획득 Mapper로 구성됨.
 * @param Configuration 하둡 환경 변수, String 임시 출력 경로.
 * @return boolean
 * @throws Exception
 */
    private boolean extractUserViewedItems_MapLoad(Configuration conf, String tmpOutputStr) throws Exception
    {
    	Job job = new Job(this.getConf());

        String outputPath = conf.get(Constants.OUTPUT_PATH) + tmpOutputStr;
        //사용자가 아이템을 평가한 자료.
        FileInputFormat.addInputPaths(job, conf.get(Constants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        FileSystem.get(conf).delete(new Path(outputPath));//FOR LOCAL TEST                   
    	logger.info("Output Path  '" + outputPath + "' will be removed ");
    	
        job.getConfiguration().set(Constants.DELIMITER, conf.get(Constants.DELIMITER, "\t"));
        job.getConfiguration().set(Constants.UID_INDEX, conf.get(Constants.UID_INDEX, "0"));
        job.getConfiguration().set(Constants.IID_INDEX, conf.get(Constants.IID_INDEX, "1"));
        job.getConfiguration().set(Constants.RATING_INDEX, conf.get(Constants.RATING_INDEX, "2"));
        job.getConfiguration().set(Constants.TARGET_UID, conf.get(Constants.TARGET_UID, null));

        job.setJarByClass(UserSimRecommenderDriver.class);

        job.setMapperClass(UserViewedItemsExtractMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.error("Error: Map Job for User's Viewed Item Information Extraction is Failed...");
            logger.info("MR-Job is Failed..");
            return false;
        }

        // extracted results > configuration loading
        String confSetStr = UserViewedItemsExtractMapper.getUserViewListString(conf, outputPath);
        //추천 대상자 레코드 저장.
        if(confSetStr!=null) conf.set(Constants.RECOMJOB_USERS_VIEWED_INFOS, confSetStr);
        else
        {
            logger.info("There is no User's Viewed Items. So, Recommendation cat not be executed more..");
            return false;
        }

        return true;
    }
/**
 * 
 * 입력 값: 사용자+구분자+아이템+평점을 포함한 문자열.
 * 출력 값 : 아이템 ID,  평균 평점(추천 점수), 유사한 사용자 수.
 * @param conf
 * @param tmpOutputStr
 * @return
 * @throws Exception
 */
    private boolean makingRecoms_MapReduce(Configuration conf, String tmpOutputStr) throws Exception
    {
    	Job job = new Job(this.getConf());

        String outputPath = conf.get(Constants.OUTPUT_PATH) + tmpOutputStr;
        FileInputFormat.addInputPaths(job, conf.get(Constants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().set(Constants.DELIMITER, conf.get(Constants.DELIMITER, "\t"));
        job.getConfiguration().set(Constants.UID_INDEX, conf.get(Constants.UID_INDEX, "0"));
        job.getConfiguration().set(Constants.IID_INDEX, conf.get(Constants.IID_INDEX, "1"));
        job.getConfiguration().set(Constants.RATING_INDEX, conf.get(Constants.RATING_INDEX, "2"));
        job.getConfiguration().set(Constants.TARGET_UID, conf.get(Constants.TARGET_UID, null));

//     추천할 아이템의 아이디 설정.
        if(conf.get(Constants.TARGET_IID_LIST, null)!=null)
            job.getConfiguration().set(Constants.TARGET_IID_LIST, conf.get(Constants.TARGET_IID_LIST, null));
       
//      유사 사용자 정보 설정.
        if(conf.get(Constants.RECOMJOB_SIMIL_USER_INFOS, null)!=null)
            job.getConfiguration().set(Constants.RECOMJOB_SIMIL_USER_INFOS, conf.get(Constants.RECOMJOB_SIMIL_USER_INFOS, null));
        
//    추천 대상 사용자의 아이템 평점정보.
        if(conf.get(Constants.RECOMJOB_USERS_VIEWED_INFOS, null)!=null)
            job.getConfiguration().set(Constants.RECOMJOB_USERS_VIEWED_INFOS, conf.get(Constants.RECOMJOB_USERS_VIEWED_INFOS, null));

//      추천할 아이템이 있는 경우.
        	if(conf.get(Constants.RECOMJOB_ITEM_DEFINED, null)!=null)
            job.getConfiguration().set(Constants.RECOMJOB_ITEM_DEFINED, conf.get(Constants.RECOMJOB_ITEM_DEFINED, null));


        job.setJarByClass(UserSimRecommenderDriver.class);

        job.setMapperClass(FinalRecommendationMakingMapper_UserSim.class);
        job.setReducerClass(FinalRecommendationMakingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.error("Error: Map/Reduce Job for Final Recommendation Making is Failed...");
            logger.info("MR-Job is Failed..");
            return false;
        }

        // final result  > outputPath
        FinalRecommendationMakingReducer.finalRecomResultWriting(conf, outputPath, "/recomResult.txt");

        return true;
    }
}
