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

package org.ankus.mapreduce.algorithms.statistics.nominalstats;

import java.io.*;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.ankus.util.Usage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
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
 * 범주 데이터의 주기와 비율을 계산
 * @version 0.0.1
 * @date : 2013.08.20
 * @author Moonie
 */
public class NominalStatsDriver extends Configured implements Tool {

    	private Logger logger = LoggerFactory.getLogger(NominalStatsDriver.class);
    	
	    /**
	    * 파라미터 설정 및 알고리즘 초기화.
	    * @param String[]  args :	알고리즘 수행을 위한 사용자 인자.
	    * @return int 정상 수행시 0, 오류발생시 1
	    * @author Moonie
	    */
	    @Override
		public int run(String[] args) throws Exception
		{
			/**
			 * 1st Job - Frequency Computation (MR)
			 * 2nd Job - Ratio Computation (By Total Record Count, Map Only)
			 */
	    	long endTime = 0;
	       	long lTime  = 0;
	       	long startTime = 0 ; 
	       	
	        logger.info("Nominal Statistics MR-Job is Started..");
	
			Configuration conf = this.getConf();
			//conf.set("fs.defaultFS",  "hdfs://localhost:9000");
			if(!ConfigurationVariable.setFromArguments(args, conf))
			{
				logger.error("MR Job Setting Failed..");
	            Usage.printUsage(Constants.DRIVER_NOMINAL_STATS);
	            logger.info("Error: MR Job Setting Failed..: Configuration Failed");
	            return 1;
			}
			startTime = System.nanoTime();
	
			String tempStr = "_freqs";
	
	        logger.info("1st-Step of MR-Job is Started..");
	        
//	        범주 데이터의 발생 빈도를 계산한다.	        
			Job job1 = new Job(this.getConf());
			set2StepJob1(job1, conf, tempStr);
	        job1.setJarByClass(NominalStatsDriver.class);
	        job1.setMapperClass(NominalStatsFrequencyMapper.class);
	        job1.setReducerClass(NominalStatsFrequencyReducer.class);	
	        job1.setMapOutputKeyClass(Text.class);
	        job1.setMapOutputValueClass(IntWritable.class);
	
	        job1.setOutputKeyClass(NullWritable.class);
	        job1.setOutputValueClass(Text.class);
	       
	        if(!job1.waitForCompletion(true))
	    	{
	        	logger.error("Error: MR(Step-1) for Nominal Stats is not Completion");
	            logger.info("MR-Job is Failed..");
	            return 1;
	        }
	        logger.info("1st-Step of MR-Job is successfully finished..");
	
//	        Counter를 사용하여 전체 데이터의 갯수를 획득한다.
	        long mapOutCnt = job1.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();	
	
	        logger.info("Final Ratio Computation and Result Integration is Starting..");
	        String inputPath = conf.get(ArgumentsConstants.OUTPUT_PATH) + tempStr;
	        String outputFile = conf.get(ArgumentsConstants.OUTPUT_PATH) + "/result";
	        
//	        최종 결과물을 파일로 기록한다.
	        finalComputation(conf, inputPath, mapOutCnt, outputFile);
	        logger.info("Final Ratio Computation and Result Integration is Finished..");
	        endTime = System.nanoTime();
			lTime = endTime - startTime;
			
//			임시 파일을 삭제한다.
	        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
	        {
	            logger.info("Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH) + tempStr);
	        	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + tempStr), true);
	        }
	        logger.info("MR-Job is successfully finished..");
	        return 0;
		}
	
	    /**
	    * 범주 데이터의 최종 결과을 저장함.
	    * @param Configuration conf  Hadoop 환경 설정 변수
	    * @param String inputPath    입력 경로
	    * @param long dataCnt        전체 개체 갯수
	    * @param String outputFile   출력 경로
	    * @author Moonie
	    */
	    private void finalComputation(Configuration conf, String inputPath, long dataCnt, String outputFile) throws Exception
	    {
	        FileSystem fs = FileSystem.get(conf);
	        String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
	        String targetIndex = conf.get(ArgumentsConstants.TARGET_INDEX, "-1");
	
	        FSDataOutputStream fout = fs.create(new Path(outputFile), true);
	        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
	        bw.write("# Attr-" + targetIndex + delimiter + "frequency" + delimiter + "ratio" + "\n");
	
	        FileStatus[] status = fs.listStatus(new Path(inputPath));
	        for (int i=0;i<status.length;i++)
	        {
	            Path fp = status[i].getPath();
	
	            if(fp.getName().indexOf("part-")==0)
	            {
	                FSDataInputStream fin = fs.open(fp);
	                BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
	
	                String readStr, tokens[];
	                while((readStr=br.readLine())!=null)
	                {
	                    tokens = readStr.split(delimiter);
	                    long freq = Long.parseLong(tokens[1]);
	                    if(freq == 0) bw.write(readStr + delimiter + "0\n");
	                    else bw.write(readStr + delimiter + ((double)freq/(double)dataCnt) + "\n");
	                }
	
	                br.close();
	                fin.close();
	            }
	        }
	        bw.close();
	        fout.close();
	    }
	    /**
	    * NominalStats를 구동하기 위한 메인 함수.
	    * @param String args[] :NominalStatsDriver를 구동하기 위한 인자.
	    * @throws Exception
	    * @author Moonie
	    */
		public static void main(String args[]) throws Exception 
		{
			int res = ToolRunner.run(new NominalStatsDriver(), args);
	        System.exit(res);
		}
		/**
		* Frequency Computation를 위한 MapReduce수행 설정.
		* @param Job job : Job 환경 식별자
		* @param Configuration conf : Job 설정 변수
		* @param Configuration outputPathStr : job 결과 출력 경로.
		* @author Moonie
		*/
		private void set2StepJob1(Job job, Configuration conf, String outputPathStr) throws IOException
		{
			FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
			FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputPathStr));
			job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
			job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
		}


}
