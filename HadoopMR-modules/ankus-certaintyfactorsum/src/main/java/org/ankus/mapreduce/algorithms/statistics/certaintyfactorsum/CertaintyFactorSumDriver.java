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

package org.ankus.mapreduce.algorithms.statistics.certaintyfactorsum;

import java.io.IOException;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.ankus.util.Usage;
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

import org.ankus.util.ConfigurationVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 확신도 기반 합계 드라이버 클래스 
 * @version 0.0.1
 * @date : 2013.08.20
 * @author Moonie
 */
public class CertaintyFactorSumDriver extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(CertaintyFactorSumDriver.class);
    /**
     * main()함수로 ToolRunner를 사용하여  확신도 기반 합계를 호출한다.
     * @version 0.0.1
	 * @date : 2013.08.20
	 * @parameter String[] args : 확신도 기반 합계 알고리즘 수행 인자.
	 * @author Moonie
     * @throws Exception
     */
    public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new CertaintyFactorSumDriver(), args);
        System.exit(res);
	}
    /**
     * ToolRunner에서 호출되는 실제 알고리즘 시작 함수.
     * @author HongJoong.Shin
     * @date 2016.12.06
     * @parameter String[] args : 확신도 기반 알고리즘 수행 인자.
     * @return int (정상 수행시 0, 오류 발생시 1)
     */
	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception
	{
		long endTime = 0;
       	long lTime  = 0;
       	long startTime = 0 ; 
        logger.info("Certainty Factor based Summation MR-Job is Started..");

        /**
		 * 1st Job - Segmentation and Local CF Summation
		 * 2nd Job - Global CF Summation
		 */
		Configuration conf = this.getConf();		
        if(!ConfigurationVariable.setFromArguments(args, conf))
		{
			logger.error("MR Job Setting Failed..");
            Usage.printUsage(Constants.DRIVER_CERTAINTYFACTOR_SUM);

            logger.info("Error: MR Job Setting Failed..: Configuration Failed");
            return 1;
		}
        startTime = System.nanoTime();
		if(conf.get(ArgumentsConstants.MR_JOB_STEP, "1").equals("1"))
		{
			FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
            logger.info("MR-Job is set to 1-Step.");
			Job job = new Job(this.getConf());
			FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
			FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
			job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
			job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
			job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
			job.getConfiguration().set(ArgumentsConstants.CERTAINTY_FACTOR_MAX, conf.get(ArgumentsConstants.CERTAINTY_FACTOR_MAX, "1"));
			
			/*
			 * 데이터를 나누지 않고 전체 데이터에 대해서 확신도 합계를 산출한다.
			 */
			job.setJarByClass(CertaintyFactorSumDriver.class);
			job.setMapperClass(CFSum1MRMapper.class);
			job.setReducerClass(CFSumReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			if(!job.waitForCompletion(true))
	    	{
	        	logger.error("Error: MR for Numeric Stats is not Completion");
                logger.info("MR-Job is Failed..");
	        	return 1;
	        }
		}
		else
		{
            logger.info("MR-Job is set to 2-Step.");
			String tempStr = "_splitSum";
            logger.info("1st-Step of MR-Job is Started..");
            FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
            Job job1 = new Job(this.getConf());
//          분산처리를 위해 데이터를 n개(hadoop 시스템의 reducer의 개수만큼)의 폴드로 나누고 각각의 데이터 폴 드별로 확신도 합을 산출한다.
			FileInputFormat.addInputPaths(job1, conf.get(ArgumentsConstants.INPUT_PATH));
			FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + tempStr));
			FileOutputFormat.setOutputPath(job1, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + tempStr));
			job1.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
			job1.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
			job1.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
			job1.getConfiguration().set(ArgumentsConstants.CERTAINTY_FACTOR_MAX, conf.get(ArgumentsConstants.CERTAINTY_FACTOR_MAX, "1"));
			job1.setJarByClass(CertaintyFactorSumDriver.class);	
	        job1.setMapperClass(CFSum2MRSplitMapper.class);
	        job1.setReducerClass(CFSumReducer.class);
	        job1.setMapOutputKeyClass(Text.class);
	        job1.setMapOutputValueClass(Text.class);
	        job1.setOutputKeyClass(NullWritable.class);
	        job1.setOutputValueClass(Text.class);
	        if(!job1.waitForCompletion(true))
            {
	        	logger.error("Error: MR(1st step) for Certainty Factor SUM is not Completion");
                logger.info("MR-Job is Failed..");
                return 1;
	        }

            logger.info("1st-Step of MR-Job is successfully finished..");
            logger.info("2nd-Step of MR-Job is Started..");	        
	        Job job2 = new Job(this.getConf());
	        FileInputFormat.addInputPaths(job2, conf.get(ArgumentsConstants.OUTPUT_PATH) + tempStr);
			FileOutputFormat.setOutputPath(job2, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
			job2.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
			job2.getConfiguration().set(ArgumentsConstants.CERTAINTY_FACTOR_MAX, conf.get(ArgumentsConstants.CERTAINTY_FACTOR_MAX, "1"));
	        
//	        1단계를 통해 n개의 폴드별로 산출된 확신도를 전체 합산하여, 전체 확신도 값을 산출한다.
	        job2.setJarByClass(CertaintyFactorSumDriver.class);
	        job2.setMapperClass(CFSum2MRMergeMapper.class);
	        job2.setReducerClass(CFSumReducer.class);
	        job2.setMapOutputKeyClass(Text.class);
	        job2.setMapOutputValueClass(Text.class);
	        job2.setOutputKeyClass(NullWritable.class);
	        job2.setOutputValueClass(Text.class);
	        if(!job2.waitForCompletion(true))
	    	{
	        	logger.error("Error: MR(2nd step) for Certainty Factor SUM is not Completion");
                logger.info("MR-Job is Failed..");
                return 1;
	        }
            logger.info("2nd-Step of MR-Job is successfully finished..");
	        // temp deletion
	        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
	        {
                logger.info("Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH) + tempStr);
	        	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + tempStr), true);
	        }
		}
		endTime = System.nanoTime();
		lTime = endTime - startTime;
		
		System.out.println("Certainty Factor based Summation Finished TIME(ms) : " + lTime/1000000.0);
		
        logger.info("MR-Job is successfully finished..");
        return 0;
        
	}
	    
}