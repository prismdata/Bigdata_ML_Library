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

package org.ankus.mapreduce.algorithms.statistics.numericstats;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.ankus.util.Usage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ankus.util.ArgumentsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 수치 통계 분석 모듈
 * 평균, 최대,최소, 표준 편차등을 산출함.
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStatsDriver extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(NumericStatsDriver.class);
    /**
    * NumericStats를 구동하기 위한 메인 함수.
    * @param String args[] :NumericStats를 구동하기 위한 인자.
    * @throws Exception
    * @author Moonie
    */
    public static void main(String args[]) throws Exception
    {
        int res = ToolRunner.run(new NumericStatsDriver(), args);
        System.exit(res);
    }

    /**
     * 일반적인 수치 통계만 추출하기 위한 job설정.
     * @param Job job : Job 환경 식별자
     * @param Configuration conf : Hadoop 환경 설정 변수
     * @param String outputPathStr 
     * @author Moonie
     */
    private void set1StepJob(Job job, Configuration conf, String outputPathStr) throws IOException
    {
        // TODO
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputPathStr));
        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
    }

    /**
     * 통계 분석 1단계인 수치 데이터 구간 분리 MR을 수행하는 Job의 환경 설정
     * @param Job job : Job 환경 식별자
     * @param Configuration conf : Hadoop 환경 설정 변수
     * @param String outputPathStr : output path for job
     * @author Moonie
     */
    private void set2StepJob1(Job job, Configuration conf, String outputPathStr) throws IOException
    {
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputPathStr));
        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
    }

	 /**
	  * GenericOptionsParser와 함께 작동하여 generic hadoop 명령 줄 인수를 구문 분석.<br>
	  * @param String[] args : 실행 파라이터.(내부 실행용)
	  * @return 정상 수행시 0, 오류 발생시 1을 리턴
	  * @throws Exception
	  * @author Moonie
	  */
	@Override
	public int run(String[] args) throws Exception
	{
		long endTime = 0;
		long lTime  = 0;
		long startTime = 0 ; 
	       	
		/**
		 * 1st Job - Segmentation and Local Computation (MR)
		 * 2nd Job - Global Computation (MR)
		 */
        logger.info("Numeric Statistics MR-Job is Started..");

        Configuration conf = this.getConf();
        
		if(!ConfigurationVariable.setFromArguments(args, conf))
		{
			logger.error("MR Job Setting Failed..");
            Usage.printUsage(Constants.DRIVER_NUMERIC_STATS);

            logger.info("Error: MR Job Setting Failed..: Configuration Error");
            return 1;
		}
		
		startTime = System.nanoTime();
        String resultPath_1 = "_res1";
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + resultPath_1), true);
        
//		mrJobStep이 값이 1인 경우. 일반적인 수치 통계만 추출함.
//      합 + 구분자 + 평균 + 구분자 + 조화 평균  + 구분자 + 기하 평균 + 구분자 +분산 + 구분자 +표준편차 + 구분자 +최대 값 + 구분자 +최소 값 + 구분자 + 전체 데이터 수;
		if(conf.get(ArgumentsConstants.MR_JOB_STEP, "2").equals("1"))
		{
            logger.info("MR-Job for Basic Stats is set to 1-Step.");

			Job job = new Job(this.getConf());
			set1StepJob(job, conf, resultPath_1);
			job.setJarByClass(NumericStatsDriver.class);
			
			job.setMapperClass(NumericStats1_1MRMapper.class);
			job.setReducerClass(NumericStats1_1MRReducer.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			if(!job.waitForCompletion(true))
	    	{
	        	logger.error("Error: MR for Numeric Stats is not Completeion");
                logger.info("MR-Job for Basic Stats is Failed..");
	        	return 1;
	        }
		}
		else
		{
			/*
			 * STEP 1
			 * 옵셋, Reducer 갯수, 컬럼 번호를 이용하여 키를 생성하고, 컬럼 번호에 해당하는 것을 값으로 사용하여 Mapper에 전송한다.
			 * 데이터 수 + 구분자 + 최대 값 + 구분자 + 최소 값 + 구분자 + 합계 + 구분자 + 하모닉 합 + 구분자 + 기하 합 + 구분자 + 스퀘어 합계 + 구분자 + (값이 양수이면 T, 아니면 F)를 생성한다.
			 */
            logger.info("MR-Job for Basic Stats is set to 2-Step.");
			String outputTempStr = "_splitStat";
			
            logger.info("1st-Step of MR-Job is Started..");
            //Job job1 = new Job(); //runtime file access error
			Job job1 = new Job(this.getConf());
			set2StepJob1(job1, conf, outputTempStr);
			job1.setJarByClass(NumericStatsDriver.class);

	        job1.setMapperClass(NumericStats1_2MRSplitMapper.class);
	        job1.setReducerClass(NumericStats1_2MRSplitReducer.class);

	        job1.setMapOutputKeyClass(Text.class);
	        job1.setMapOutputValueClass(Text.class);

	        job1.setOutputKeyClass(Text.class);
	        job1.setOutputValueClass(Text.class);

	        if(!job1.waitForCompletion(true))
	    	{
	        	logger.error("Error: MR(1st step) for Numeric Stats is not Completion");
                logger.info("MR-Job is Failed..");
                return 1;
	        }

            logger.info("1st-Step of MR-Job is successfully finished..");
            logger.info("2nd-Step of MR-Job is Started..");
	       
	        Job job2 = new Job(this.getConf());
	        /*
	         * STEP 2
	        * 1단계 기초 통계 결과를 로드하여 컬럼번호, 통계 자료를 key, value로 구조화
	        * 컬럼 번호, 합, 평균, 조화평균,기하 평균, 분산, 표준 편차, 최대, 최소, 데이터 수 산출.
	         */
	        FileInputFormat.addInputPaths(job2, conf.get(ArgumentsConstants.OUTPUT_PATH) + outputTempStr);
	        FileOutputFormat.setOutputPath(job2, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + resultPath_1));
	        job2.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
	        job2.setJarByClass(NumericStatsDriver.class);
	        job2.setMapperClass(NumericStats1_2MRMergeMapper.class);
	        job2.setReducerClass(NumericStats1_2MRMergeReducer.class);

	        job2.setMapOutputKeyClass(Text.class);
	        job2.setMapOutputValueClass(Text.class);

	        job2.setOutputKeyClass(NullWritable.class);
	        job2.setOutputValueClass(Text.class);

	        if(!job2.waitForCompletion(true))
	    	{
	        	logger.error("Error: MR(2nd step) for Numeric Stats is not Completion");
                logger.info("MR-Job is Failed..");
                return 1;
	        }

            logger.info("2nd-Step of MR-Job is successfully finished..");
	        
	        // temp deletion
	        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
	        {
                logger.info("Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH) + outputTempStr);
	        	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputTempStr), true);
	        }
		}
        logger.info("MR-Job for Basic Stats is successfully finished..");
        logger.info("MR-Job for Quartiles is Started....");
        logger.info("1st MR-Job for Quartiles (Block Position Setting) is Started....");
        String outputTmp2_1 = "_2_1_BlockPos";
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputTmp2_1), true);
        
        CounterGroup cg;
        /*
        입력 : 컬럼 번호, 합, 평균, 조화평균,기하 평균, 분산, 표준 편차, 최대, 최소, 데이터 수
        4분위를 구하기 위한 데이터 전처리 수행
        emit
        key : null
        value : 컬럼번호 + "-{1B, 2B, 3B, 4B}" + 컬럼 값.
         */
        if((cg=execBlockPositionSetMRJob(conf, resultPath_1, outputTmp2_1))==null)
        {
            logger.error("1st MR-Job for Quartiles (Block Position Setting) is Failed...");
            return 1;
        }        
        
        logger.info("1st MR-Job for Quartiles (Get Quatiles) is Started....");
        String outputTmp2_2 = "_2_2_BlockPos";
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputTmp2_2), true);
        /*
         * 입력 데이터:  컬럼번호 + "-{1B, 2B, 3B, 4B}" + 컬럼 값
         */
        if(!getQuatilesInfoMRJob(conf, resultPath_1, outputTmp2_1, cg, outputTmp2_2))
        {
            logger.error("2nd MR-Job for Quartiles (Get Quatiles) is Failed...");
            return 1;
        }
        
        // final merge and computation
        logger.info("Final Computation and Result Integration is Starting..");
        String finalOutputFile = "/result";
        if(!finalComputationAndGeneration(conf, resultPath_1, outputTmp2_2, finalOutputFile))
        {
            logger.error("Final Result Integration is Failed...");
            return 1;
        }
       
        endTime = System.nanoTime();
		lTime = endTime - startTime;
		
		System.out.println("Numeric Statistic Analysis Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
		System.out.format("Numeric Statistic Analysis Finished Time : %f Seconds\n", (lTime/1000000.0)/1000);
        // tempDelete : resultPath_1 , outputTmp2_1 , outputTmp2_2
        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            logger.info("Temporary Files are Deleted..: ");
            FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + resultPath_1), true);
            FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputTmp2_1), true);
            FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputTmp2_2), true);
        }

        return 0;
	}
	/**
	 * 최종 결과를 파일로 출력함.
	 * @param Configuration conf : Hadoop 환경 설정 변수
	 * @param String inputPath1 : 기본 통계 결과를 가지고 있는 경로.
	 * @param String inputPath2 : 4분위수 결과를 가지고 있는 경로.
	 * @param String outputFile : 통합 결과를 저장할 경로.
	 * @return 성공시 true, 오류 발생시 false를 리턴함
	 * @throws Exception
	 * @author Moonie
	 */
    private boolean finalComputationAndGeneration(Configuration conf, String inputPath1, String inputPath2, String outputFile) throws Exception
    {
        HashMap<String, Double> quartileMap = getFinalQuartileValues(conf, conf.get(ArgumentsConstants.OUTPUT_PATH) + inputPath2);

        if(quartileMap==null) return false;

        String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream fout = fs.create(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputFile), true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
        bw.write("# AttrIndex" + delimiter +
                    "Sum" + delimiter +
                    "Average" + delimiter +
                    "HarmonicAverage" + delimiter +
                    "GeographicAverage" + delimiter +
                    "Variance" + delimiter +
                    "StandardDeviation" + delimiter +
                    "Max" + delimiter +
                    "Min" + delimiter +
                    "DataCnt" + delimiter +
                    "1stQuartile" + delimiter +
                    "2ndQuartile" + delimiter +
                    "3rdQuartile" + "\r\n");

        FileStatus[] status = fs.listStatus(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + inputPath1));
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

                    String qStr = quartileMap.get(tokens[0] + "-1Q") + delimiter +
                                    quartileMap.get(tokens[0] + "-2Q") + delimiter +
                                    quartileMap.get(tokens[0] + "-3Q");
                    bw.write(readStr + delimiter + qStr + "\r\n");
                }

                br.close();
                fin.close();
            }
        }

        bw.close();
        fout.close();
        return true;
    }
    /**
     * 4분위수를 파일로 부터 메모리에 적재하는 기능
     * @param Configuration conf : Hadoop 환경 설정 변수
	 * @param String inputPath : 4분위수가 저장된 파일 경로
	 * @return 4분위수가 저장된 HashMap 변수
	 * @throws Exception
	 * @author Moonie
     */
    private HashMap<String, Double> getFinalQuartileValues(Configuration conf, String inputPath) throws Exception
    {
        HashMap<String, Double> retMap = new HashMap<String, Double>();
        String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

        FileSystem fs = FileSystem.get(conf);
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
                    double curVal = Double.parseDouble(tokens[1]);
                    if(retMap.containsKey(tokens[0]))
                    {
                        double prevVal = retMap.get(tokens[0]);
                        curVal = (prevVal + curVal) / 2;
                    }
                    retMap.put(tokens[0], curVal);
                }
                br.close();
                fin.close();
            }
        }
        if(retMap.size() == 0) return null;
        return retMap;
    }
	/**
	 * 각 컬럼의 분위별 데이터 값 목록을 받아 4분위수를 출력함.
	 * 입력 데이터:  컬럼번호 + "-{1B, 2B, 3B, 4B}" + 컬럼 값
	 * @param Configuration conf : 하둡 환경 변수.
	 * @param String preResultPathStr : 기초 통계 자료 경로.
	 * @param String inputPathStr  : 전처리된 4분위 값을 가진 경로
	 * @param CounterGroup cg : 카운터 객체 들을 가진 변수.
	 * @param String outputPathStr : 출력 결과 경로.
	 * @return boolean : 성공(true),실폐(false)
	 * @author Moonie
	 */
    private boolean getQuatilesInfoMRJob(Configuration conf, String preResultPathStr, String inputPathStr, CounterGroup cg, String outputPathStr) throws Exception
    {
    	//Job job = new Job(); //runtime error when file access
    	Job job = new Job(this.getConf());
        
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.OUTPUT_PATH) + inputPathStr);
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputPathStr));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.OUTPUT_PATH, conf.get(ArgumentsConstants.OUTPUT_PATH));
        extractBlockInfoToConf(job.getConfiguration(), preResultPathStr);     
        //속성별 각 분위에 속하는 데이터의 갯수를 카운터에서 읽고 속성변호, 값의 갯수로 Job에 설정함.
        Iterator<Counter> iter = cg.iterator();
        while(iter.hasNext())
        {
            Counter c = iter.next();
            System.out.println(c.getName());
            System.out.println(c.getValue());
            job.getConfiguration().set(c.getName(), c.getValue() + "");
        }
        job.setJarByClass(NumericStatsDriver.class);
        job.setMapperClass(NumericStats2_QuartilesMapper.class);
        job.setReducerClass(NumericStats2_QuartilesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.error("Error: MR-Job for Quartiles (Get Quatiles) is Failed...");
            logger.info("MR-Job is Failed..");
            return false;
        }
        return true;
    }

    /**
     * 전처리 과정으로 입력 데이터를 4개 구간으로 분리하기 위해 데이터에 4분위 구간 정보를 설정하여 출력한다.
     *  input : 입력 데이터
     *  output : 컬럼 번호 + "-1,2,3,4B" + 구분자 + 입력 데이터의 컬럼 값
     * @param Configuration conf
     * @param String preResultPathStr 컬럼 번호, 합, 평균, 조화평균,기하 평균, 분산, 표준 편차, 최대, 최소, 데이터 수 산출.
     * @param String outputPathStr 4분위를 구하기 위한 데이터 전처리 결과 저장 경로
     * @return CounterGroup :각 분위별 데이터의 갯수 
     * @throws Exception
     * @author Moonie
     */
    private CounterGroup execBlockPositionSetMRJob(Configuration conf, String preResultPathStr, String outputPathStr) throws Exception
    {

    	Job job = new Job(this.getConf());
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputPathStr));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));

        job.getConfiguration().set(ArgumentsConstants.OUTPUT_PATH, conf.get(ArgumentsConstants.OUTPUT_PATH));
        
        extractBlockInfoToConf(job.getConfiguration(), preResultPathStr);

        job.setJarByClass(NumericStatsDriver.class);
        job.setMapperClass(NumericStats2_BlockInfoMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.error("Error: MR-Job for Quartiles (Block Position Setting) is Failed...");
            logger.info("MR-Job is Failed..");
            return null;
        }

        return job.getCounters().getGroup(Constants.STATS_NUMERIC_QUARTILE_COUNTER);
    }

    /**
     * 컬럼 번호, 합, 평균, 조화평균,기하 평균, 분산, 표준 편차, 최대, 최소, 데이터 수 산출에서 <br>
     * 컬럼 번호 : 최소, 평균, 최대를 환경변수에 저장.
     * @param Configuration conf : 환경 변수
     * @param String inputPathStr : 통계 정보를 가진 파일 경로.
     * @throws Exception
     * @author Moonie
     */
    private void extractBlockInfoToConf(Configuration conf, String inputPathStr) throws Exception
    {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + inputPathStr));
        String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

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
                    String confKeyStr = tokens[0] + "Block"; //컬럼번호
                    double block2 = Double.parseDouble(tokens[2]); //평균 
                    double block1 = (block2 + Double.parseDouble(tokens[8]))/2.0; //최소 
                    double block3 = (block2 + Double.parseDouble(tokens[7]))/2.0; //최대 
                    String cntStr = tokens[9]; //데이터수
                    System.out.println("Attr " + confKeyStr +":" + block1 + "," + block2 + "," + block3 + "," + cntStr);
                    conf.set(confKeyStr, block1 + "," + block2 + "," + block3 + "," + cntStr);
                }

                br.close();
                fin.close();
            }
        }
    }
}
