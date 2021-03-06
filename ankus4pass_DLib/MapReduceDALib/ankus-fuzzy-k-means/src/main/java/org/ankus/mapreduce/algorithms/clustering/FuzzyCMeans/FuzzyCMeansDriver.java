package org.ankus.mapreduce.algorithms.clustering.FuzzyCMeans;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.ArrayList;
import java.util.List;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.ankus.util.Usage;
import org.apache.hadoop.conf.Configuration;

/**
 * 퍼지 군집 분석을 수행하는 클래스
 * @author HongJoong.Shin
 * @date 2018. 1. 19.
 */
public class FuzzyCMeansDriver extends Configured implements Tool {
	private Logger logger = LoggerFactory.getLogger(FuzzyCMeansDriver.class);
	/**
     * main()함수로 ToolRunner를 사용하여 군집 분석 기능을 호출한다.
     * @author  HongJoong.Shin
     * @param String[] args  : 군집 분석 알고리즘 수행 인자.
     * @date   2016.12.06
     * @return 없음.
     */
	public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new FuzzyCMeansDriver(), args);
        System.exit(res);
	}
	/**
     * ToolRunner에서 호출되는 실제 알고리즘 시작 함수.
     * @auth HongJoong.Shin
     * @date 2016.12.06
     * @param String[] args : 군집 분석 알고리즘 수행 인자.
     * @return int 정상 종료시 0, 오류 발생시 1을 리턴함.
     */
	@Override
	public int run(String[] args) throws Exception 
	{
	 	logger.info("FuzzyKMeans Clustering MR-Job is Started..");
	 	FileStatus[] status = null;
		Path path = null;
		FileSystem fs  = null;
		String Source_path = "";
		Configuration conf = this.getConf();
		long endTime = 0;
       	long lTime  = 0;
       	long startTime = 0 ; 
       	
		if(!ConfigurationVariable.setFromArguments(args, conf))
		{
            Usage.printUsage(Constants.DRIVER_FuzzyKMEANS_CLUSTERING);
            logger.info("FuzzyKMeans Clustering MR-Job is Failed..: Configuration Failed");
            return 1;
		}
		fs = FileSystem.get(conf);
		startTime = System.nanoTime();
		Job jobRandomWeight = new Job(this.getConf());
		Source_path = conf.get(ArgumentsConstants.INPUT_PATH);
        FileInputFormat.addInputPaths(jobRandomWeight, conf.get(ArgumentsConstants.INPUT_PATH));
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/Weight0"), true);//FOR LOCAL TEST
		FileOutputFormat.setOutputPath(jobRandomWeight, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/Weight0"));
		jobRandomWeight.setJarByClass(FuzzyCMeansDriver.class);
		jobRandomWeight.setMapperClass(Mapper_Random_Weight.class);
		jobRandomWeight.setReducerClass(Reducer_Random_Reducer.class);
		jobRandomWeight.setOutputKeyClass(Text.class);
		jobRandomWeight.setOutputValueClass(Text.class);
        if(!jobRandomWeight.waitForCompletion(true))
        {
            logger.info("Error: FuzzyKMeans-RandomWeight is not Completeion");
            return 1;
        }
	        	
        List<String> beforeCentroid = new ArrayList<String>();
        int i =0;	       
        //Get Centroid...
        long max_iteration = conf.getLong(ArgumentsConstants.Fuzzy_CMeans_MAXITERATION, 10);
        
        while(i < max_iteration)
        {
	        Job jobGetCentroid = new Job(this.getConf());	
	        String StrWeight = conf.get(ArgumentsConstants.OUTPUT_PATH)+"/Weight" + i;
	        /**
	         * 군집이 가진 속성별 가중치를 계산한다.
	         */
	        FileInputFormat.addInputPaths(jobGetCentroid, StrWeight);
	        conf.setInt("iteration_count", i);
	        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/Centroid"), true);//FOR LOCAL TEST
			FileOutputFormat.setOutputPath(jobGetCentroid, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/Centroid"));
	        jobGetCentroid.setJarByClass(FuzzyCMeansDriver.class);
	        jobGetCentroid.setMapperClass(Mapper_Centroid.class);
	        jobGetCentroid.setReducerClass(Reducer_Centroid.class);
	        jobGetCentroid.setOutputKeyClass(Text.class);
	        jobGetCentroid.setOutputValueClass(Text.class);
	        if(!jobGetCentroid.waitForCompletion(true))
	        {
	            logger.info("Error: FuzzyKMeans-jobGetCentroid is not Completeion");
	            return 1;
	        }
	        
	        //Get new Membership
	        Job jobGetMembership = new Job(this.getConf());
	        i++;
	        List<String> UpdateCentroid = new ArrayList<String>();

	        /**
	         * 군집의 가중치 정보를 분산 캐쉬에 저장한다.
	         */
	        String Centroid_Path = conf.get(ArgumentsConstants.OUTPUT_PATH)+"/Centroid";
	        fs = FileSystem.get(conf);
			path = new Path(Centroid_Path);
			status = fs.listStatus(path);
			for (int fsi=0; fsi<status.length; fsi++)
			{
				Path ipath = status[fsi].getPath();
				logger.info("FList path : " + ipath.getName());
				if(ipath.getName().indexOf("part-r") >= 0)
				{
					FSDataInputStream fin = fs.open(status[fsi].getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));	            
					String readStr, tokens[];
					while((readStr=br.readLine())!=null)
					{
						UpdateCentroid.add(readStr);
					}
					br.close();
					fin.close();
					String strpath = Centroid_Path + "/"+ipath.getName();
					DistributedCache.addCacheFile(new Path(strpath).toUri(), jobGetMembership.getConfiguration());
				}
			}
			/**
			 * 군집의 중심 변경 여부를 확인한다.
			 * 중심이 변하지 않으면 군집화를 중지한다. 
			 */
			boolean stop_condition = true;
			String before = "";
			String after ="";
			if(beforeCentroid.size() > 0)
			{
				for(int ui = 0; ui  < UpdateCentroid.size(); ui++)
				{
					before = beforeCentroid.get(ui);
					after = UpdateCentroid.get(ui);
					if(!beforeCentroid.get(ui).equals(UpdateCentroid.get(ui)))
					{
						stop_condition = false;
						break;
					}
				}
				beforeCentroid.clear();
				for(int ui = 0; ui  < UpdateCentroid.size(); ui++)
				{
					String newCetroid = UpdateCentroid.get(ui);
					beforeCentroid.add(newCetroid);
				}				
				if(stop_condition == true)
				{
					break;
				}
			}
			else
			{
				for(int ui = 0; ui  < UpdateCentroid.size(); ui++)
				{
					beforeCentroid.add(UpdateCentroid.get(ui));
				}
				stop_condition = false;
			}
			
	        Source_path = conf.get(ArgumentsConstants.INPUT_PATH);
	        FileInputFormat.addInputPaths(jobGetMembership, Source_path);
	        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/Weight"+i), true);//FOR LOCAL TEST
			FileOutputFormat.setOutputPath(jobGetMembership, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/Weight" + i));
			jobGetMembership.setJarByClass(FuzzyCMeansDriver.class);
			jobGetMembership.setMapperClass(Mapper_Membership.class);
			jobGetMembership.setOutputKeyClass(Text.class);
			jobGetMembership.setOutputValueClass(Text.class);
			jobGetMembership.setNumReduceTasks(0);
	        if(!jobGetMembership.waitForCompletion(true))
	        {
	            logger.info("Error: Get Membership is not Completeion");
	            return 1;
	        }
	        
	        logger.info("loop continue");
        }
        
        Job jobClusterAssign = new Job(this.getConf());
        i--;
        Source_path = conf.get(ArgumentsConstants.OUTPUT_PATH)+"/Weight"+i;
        FileInputFormat.addInputPaths(jobClusterAssign, Source_path);
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/finalResult"), true);//FOR LOCAL TEST
		FileOutputFormat.setOutputPath(jobClusterAssign, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/finalResult"));
		jobClusterAssign.setJarByClass(FuzzyCMeansDriver.class);
		jobClusterAssign.setMapperClass(Mapper_ClusterAssign.class);
		jobClusterAssign.setOutputKeyClass(Text.class);
		jobClusterAssign.setOutputValueClass(Text.class);
		jobClusterAssign.setNumReduceTasks(0);
        if(!jobClusterAssign.waitForCompletion(true))
        {
            logger.info("Error: FuzzyKMeans-jobClusterAssign is not Completeion");
            return 1;
        }
        logger.debug("Repeate:" + i);
        logger.info("Exit");
        endTime = System.nanoTime();
		lTime = endTime - startTime;
		for(int itri = 0; itri <= max_iteration; itri++)
		{
			FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/Weight"+itri));
		}
		System.out.println("FuzzyKMeans Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
		return 0;			
	}	
}
