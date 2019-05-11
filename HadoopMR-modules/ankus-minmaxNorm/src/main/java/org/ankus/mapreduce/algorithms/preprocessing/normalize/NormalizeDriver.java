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

package org.ankus.mapreduce.algorithms.preprocessing.normalize;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.ankus.mapreduce.algorithms.statistics.numericstats.NumericStatsDriver;
import org.ankus.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

/**
 * 최대 최소값 정규화를 위한 드라이버 클래스 
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NormalizeDriver extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(NormalizeDriver.class);
	 /**
     * ToolRunner에서 호출되는 실제 알고리즘 시작 함수.
     * @auth Moonie
     * @date : 2013.08.21
     * @parameter String[] args : 최대 최소값 정규화 알고리즘 수행 인자.
     * @return int
     */
	@Override
	public int run(String[] args) throws Exception
	{
        logger.info("Normalization MR-Job is Started..");
		
		Configuration conf = this.getConf();
		if(!ConfigurationVariable.setFromArguments(args, conf))
		{
            Usage.printUsage(Constants.DRIVER_NORMALIZE);
            logger.info("Error: MR Job Setting Failed..: Configuration Failed");
            return 1;
		}
		
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));//FOR LOCAL TEST                   
		String result = "";
		result = conf.get(ArgumentsConstants.OUTPUT_PATH)+"_numericStat_res1";
        FileSystem.get(conf).delete(new Path(result));
        
        result = conf.get(ArgumentsConstants.OUTPUT_PATH)+"_numericStat_2_1_BlockPos";
        FileSystem.get(conf).delete(new Path(result));
        
        result = conf.get(ArgumentsConstants.OUTPUT_PATH)+"_numericStat_2_2_BlockPos";
        FileSystem.get(conf).delete(new Path(result));
        
        logger.info("Numeric Statistics for getting Min/Max value MR-Job is started..");
		
		String statJobOutput = conf.get(ArgumentsConstants.OUTPUT_PATH, null) + "_numericStat";
//		최대/최소값을 얻는 수치통계 분석용 Job을 수행하기 위한 인자 설정 
		String argsForStat[] = getParametersForStatJob(conf, statJobOutput);
		
//		수치 통계 분석 Job 수행 
        int res = ToolRunner.run(new NumericStatsDriver(), argsForStat);
        if(res!=0)
        {
            logger.info("Numeric Statistics for getting Min/Max value MR-Job is Failed..");
            return 1;
        }

        logger.info("Normalization Step of MR-Job is Started..");

        Job job = new Job(this.getConf());        
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
		job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
		job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.REMAIN_FIELDS, conf.get(ArgumentsConstants.REMAIN_FIELDS, "true"));
		//수치 통계 분석 결과로 부터 최대/최소 값을 획득하여 환경 변수에 저장.
        setMinMax(job.getConfiguration(), statJobOutput);

        job.setJarByClass(NormalizeDriver.class);
        job.setMapperClass(NormalizeMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        if(!job.waitForCompletion(true))
    	{
        	logger.error("Error: MR for Normalization is not Completion");
            logger.info("MR-Job is Failed..");
        	return 1;
        }
        
        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            logger.info("Temporary Files are Deleted..: " + statJobOutput);
        	FileSystem.get(conf).delete(new Path(statJobOutput), true);
        }
        logger.info("Normalization Step of MR-Job is Successfully Finished...");
        return 0;
	}
	
	/**
	 * 수치 통계 분석 결과를 파싱하여 각 컬럼의 최대 최소 값을 하둡 환경 변수에 저장한다.
	 * @version 0.0.1
	 * @param conf : 하둡 환경 변수 
	 * @param outputDirPath : 수치 통계 분석 결과 폴더 
	 * @return
	 * @throws Exception
	 */
    public String setMinMax(Configuration conf, String outputDirPath) throws Exception
    {
        String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path(outputDirPath + "/result");
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(filePath), Constants.UTF8));

        String readStr, tokens[];
        String indexStr = "";
        br.readLine();
        while((readStr=br.readLine())!=null)
        {
            tokens = readStr.split(delimiter);
            conf.set(Constants.STATS_MINMAX_VALUE + "_" + tokens[0], tokens[8] + "," + tokens[7]);
            indexStr += "," + tokens[0];
        }
        br.close();

        if(indexStr.length() > 0) return indexStr.substring(1);
        else return indexStr;
    }
    
    /**
     * 최대/최소값을 얻는 수치통계 분석용 Job을 수행하기 위한 인자 설정 
     * @parameter
     *      Configuration conf : 하둡 환경 설정 변수 
     *      outputPath : Job의 출력 경로 설정.
     * @return 인자가 저장된 문자열 배열 
     */
	private String[] getParametersForStatJob(Configuration conf, String outputPath) throws Exception 
	{
		String params[] = new String[14];
		
		params[0] = ArgumentsConstants.INPUT_PATH;
		params[1] = conf.get(ArgumentsConstants.INPUT_PATH, null);
		
		params[2] = ArgumentsConstants.OUTPUT_PATH;
		params[3] = outputPath;
		
		params[4] = ArgumentsConstants.DELIMITER;
		params[5] = conf.get(ArgumentsConstants.DELIMITER, "\t");
		
		params[6] = ArgumentsConstants.TARGET_INDEX;
		params[7] = conf.get(ArgumentsConstants.TARGET_INDEX, "-1");
		
		params[8] = ArgumentsConstants.EXCEPTION_INDEX;
		params[9] = conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1");
		
		params[10] = ArgumentsConstants.TEMP_DELETE;
		params[11] = conf.get(ArgumentsConstants.TEMP_DELETE, "true");
		
		params[12] = ArgumentsConstants.MR_JOB_STEP;
		params[13] = conf.get(ArgumentsConstants.MR_JOB_STEP, "1");
		
		return params;
	}
	
	/**
	 * 드라이버 클래스를 수행시키는 메인 함수로 ToolRunner를 호출함.
	 * @version 0.0.1
	 * @date : 2013.08.21
	 * @param String args[] : 알고리즘 수행 인자.
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new NormalizeDriver(), args);
        System.exit(res);
	}
}
