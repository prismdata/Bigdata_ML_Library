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

package org.ankus.mapreduce.algorithms.recommendation.recommender.itembased;

import org.ankus.mapreduce.algorithms.recommendation.recommender.commons.FinalRecommendationMakingReducer;
import org.ankus.mapreduce.algorithms.recommendation.recommender.commons.UserViewedItemsExtractMapper;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * 아이템 기반 추천 알고리즘 실행 클래스.
 * @author Wonmoon
 * @date :  2015. 1. 15
 */

public class ItemSimRecommenderDriver extends Configured implements Tool {

    // SLF4J Logging
    private Logger logger = LoggerFactory.getLogger(ItemSimRecommenderDriver.class);
    /**
     * main()함수로 ToolRunner를 사용하여 추천 알고리즘을 호출한다.
     * @author Wonmoon
     * @param String[] args : 추천 알고리즘 수행 인자.
     * @return
     */
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new ItemSimRecommenderDriver(), args);
        System.exit(res);
    }
    /**
     * ToolRunner에서 호출되는 실제 알고리즘 시작 함수.
     * @author Wonmoon
     * @param String[] args : 추천 알고리즘 수행 인자.
     * @return 정상 종료시 0, 오류 발생시 1
     */
    @Override
    public int run(String[] args) throws Exception
    {
        logger.info("Item Similarity/Correlation based Recommendation is Started..");

        // configuration setting
        Configuration conf = this.getConf();
//        conf.set("fs.default.name",  "hdfs://localhost:9000");
        if(!ConfigurationVariable.setFromArguments(args, conf))
        {
            logger.error("Job Setting Failed..");
            logger.info("Error: Job Setting Failed..: Configuration Error");
            return 1;
        }
        String tmpOutput_userViews = "_userViews";
        String tmpOutput_userUnViews = "_userUnView";
        String tmpOutput_finalReduce = "_finalReduce";
        
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) +tmpOutput_userViews));                   
    	logger.info("Output Path  '" + conf.get(ArgumentsConstants.OUTPUT_PATH) +tmpOutput_userViews + "' will be removed ");
    	
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) +tmpOutput_userUnViews));                   
    	logger.info("Output Path  '" + conf.get(ArgumentsConstants.OUTPUT_PATH) +tmpOutput_userUnViews + "' will be removed ");
    	
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) +tmpOutput_finalReduce));                   
    	logger.info("Output Path  '" + conf.get(ArgumentsConstants.OUTPUT_PATH) +tmpOutput_finalReduce + "' will be removed ");
	
        // process and mr-job
        boolean isOK = true;        

        if(!extractUserViewedItems_MapLoad(conf, tmpOutput_userViews)) isOK = false;
        else
        {
            if(conf.get(ArgumentsConstants.TARGET_IID_LIST, null)==null)
            {
                conf.set(Constants.RECOMJOB_ITEM_DEFINED, "false");
                if(!extractUserUnViewItems_MapCached(conf, tmpOutput_userUnViews)) isOK = false;
            }
            else conf.set(Constants.RECOMJOB_ITEM_DEFINED, "true");

            if(isOK && !makingRecoms_MapReduce(conf, tmpOutput_userUnViews, tmpOutput_finalReduce)) isOK = false;
        }

        // temp-delete
        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpOutput_userViews), true);
            FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpOutput_userUnViews), true);
            FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpOutput_finalReduce), true);

            logger.info("Temporary Files are Deleted..");
        }

        if(isOK)
        {
            logger.info("Item Similarity/Correlation based Recommendation is Finished..");
            return 0;
        }
        else return 1;
    }
    /**
     * 사용자가 평가한 아이템 목록을 출력
     * @author Wonmoon
     * @param Configuration conf : 하둡 환경 설정 변수. 
     * @param String tmpOutputStr : 입력 데이터 경로.
     * @return 정상 종료시 true, 오류 발생시 false
     * @throws Exception
     */
    
    private boolean extractUserViewedItems_MapLoad(Configuration conf, String tmpOutputStr) throws Exception
    {
    	Job job = new Job(this.getConf());

        String outputPath = conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpOutputStr;
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.UID_INDEX, conf.get(ArgumentsConstants.UID_INDEX, "0"));
        job.getConfiguration().set(ArgumentsConstants.IID_INDEX, conf.get(ArgumentsConstants.IID_INDEX, "1"));
        job.getConfiguration().set(ArgumentsConstants.RATING_INDEX, conf.get(ArgumentsConstants.RATING_INDEX, "2"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_UID, conf.get(ArgumentsConstants.TARGET_UID, null));

        job.setJarByClass(ItemSimRecommenderDriver.class);
        
//        [입력] 사용자 아이템 평점 레코드
//        [출력] 사용자, 지정한 아이템, 평점
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

        if(confSetStr!=null) conf.set(Constants.RECOMJOB_USERS_VIEWED_INFOS, confSetStr);
        else
        {
            logger.info("There is no User's Viewed Items. So, Recommendation cat not be executed more..");
            return false;
        }

        return true;
    }

    /**
     * 입력 데이터(아이템 평점 데이터로 부터 사용자가 구매하지 않은 아이템을 출력함.
     * @author Wonmoon
     * @param Configuration conf :하둡 환경 설정 변수
     * @param String tmpOutputStr : 아이템 출력 경로
     * @return 정상 종료시 true, 오류 발생시 false
     */
    private boolean extractUserUnViewItems_MapCached(Configuration conf, String tmpOutputStr) throws Exception
    {
    	Job job = new Job(this.getConf());

        String outputPath = conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpOutputStr;
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.UID_INDEX, conf.get(ArgumentsConstants.UID_INDEX, "0"));
        job.getConfiguration().set(ArgumentsConstants.IID_INDEX, conf.get(ArgumentsConstants.IID_INDEX, "1"));
        job.getConfiguration().set(ArgumentsConstants.RATING_INDEX, conf.get(ArgumentsConstants.RATING_INDEX, "2"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_UID, conf.get(ArgumentsConstants.TARGET_UID, null));
        job.getConfiguration().set(Constants.RECOMJOB_USERS_VIEWED_INFOS, conf.get(Constants.RECOMJOB_USERS_VIEWED_INFOS, null));

        job.setJarByClass(ItemSimRecommenderDriver.class);
//        [입력] 사용자, 아이템, 평점
//        [출력] 아이템 
        job.setMapperClass(UserUnViewedItemsExtractMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.error("Error: Map Job for User's Un-Viewed Item Information Extraction is Failed...");
            logger.info("MR-Job is Failed..");
            return false;
        }
        return true;
    }

    /**
     * 아이템간 유사도와 사용자가 구매한 아이템을 비교하여 유사한 아이템과, 평점 정보를 출력
     * @author Wonmoon
     * @param Configuration conf : 하둡 환경 설정 변수
     * @param String tmpUserUnViewOutputStr : 사용자가 구매하지 않은 데이터의 경로
     * @param String tmpOutputStr : 추천 결과의 저장 경로
     * @return 정상 종료시 true, 오류 발생시 false
     */
    private boolean makingRecoms_MapReduce(Configuration conf, String tmpUserUnViewOutputStr, String tmpOutputStr) throws Exception
    {
    	Job job = new Job(this.getConf());

        String outputPath = conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpOutputStr;
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.SIMILARITY_PATH));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.SIMILARITY_DELIMITER, conf.get(ArgumentsConstants.SIMILARITY_DELIMITER, "\t"));
        job.getConfiguration().set(Constants.RECOMJOB_USERS_VIEWED_INFOS, conf.get(Constants.RECOMJOB_USERS_VIEWED_INFOS, null));
        job.getConfiguration().set(Constants.RECOMJOB_ITEM_DEFINED, conf.get(Constants.RECOMJOB_ITEM_DEFINED, null));

        if(conf.get(Constants.RECOMJOB_ITEM_DEFINED, "false").equals("true"))
        {
            job.getConfiguration().set(ArgumentsConstants.TARGET_IID_LIST, conf.get(ArgumentsConstants.TARGET_IID_LIST, null));
        }
        else
        {
        	//사용자가 구매하지 않은 데이터를 분산 캐취에 저장함.
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + tmpUserUnViewOutputStr));
            for(int i=0; i<status.length; i++)
            {
                if(!status[i].getPath().toString().contains("part-")) continue;
                DistributedCache.addCacheFile(new URI(status[i].getPath().toString()), job.getConfiguration());
            }
        }

        job.setJarByClass(ItemSimRecommenderDriver.class);

//     	[입력] 연관 아이템1, 연관 아이템2, 유사도
//     	[출력] Key : 연관 아이템, Value : 사용자가 구매한 아이템의 평점 + 사용자가 구매한 아이템 아이디 + 유사도
        job.setMapperClass(FinalRecommendationMakingMapper_ItemSim.class);
       
//      [입력] Key : 연관 아이템, Value : 사용자가 구매한 아이템의 평점 + 사용자가 구매한 아이템 아이디 + 유사도
//      [출력] Key: Null, Value : 연관 아이템 + 사용자가 구매한 아이템의 평균 평점 + 아이템 갯수
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
        FinalRecommendationMakingReducer.finalRecomResultWriting(conf, outputPath, "/recomResult");
        return true;
    }
}