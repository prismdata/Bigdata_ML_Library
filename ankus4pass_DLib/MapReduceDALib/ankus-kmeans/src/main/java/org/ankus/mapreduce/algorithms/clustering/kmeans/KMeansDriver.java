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

package org.ankus.mapreduce.algorithms.clustering.kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import org.ankus.mapreduce.algorithms.clustering.common.ClusterCommon;
import org.ankus.mapreduce.algorithms.preprocessing.normalize.NormalizeDriver;
import org.ankus.mapreduce.algorithms.preprocessing.normalize.NormalizeMapper;
import org.ankus.mapreduce.algorithms.statistics.nominalstats.NominalStatsDriver;
import org.ankus.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * kMeans 군집  드라이버 클래스.
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie & whitepoo
 */
/*
 * <p>Modeling commend
 * <br>hadoop jar ./ankus-core2-kmeans-1.1.0.jar KMeans -input /user/demo/data/iris.csv -output /user/demo/result/kmeans_model -delimiter , -indexList 0,1,2,3 -clusterCnt 4 -convergeRate 0 -maxIteration 100 -finalResultGen true -normalize true
 * <p>Testing commend
 * <br>hadoop jar ankus-core2-kmeans-1.1.0.jar KMeans -input /user/demo/data/iris.csv -modelPath /user/demo/result/kmeans_model/ -output /user/demo/result/kmeans_test -delimiter , -indexList 0,1,2,3 -clusterCnt 4 -convergeRate 0 -maxIteration 100 -finalResultGen true
 */
public class KMeansDriver extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(KMeansDriver.class);

	private int mIndexArr[];                      // attribute index array will be used clustering
	private int mNominalIndexArr[];              // nominal attribute index array will be used clustering
	private int mExceptionIndexArr[];           // attribute index array will be not used clustering

    // TODO: convergence rate
    private double mConvergeRate = 0.001;
    private int mIterMax = 100;
    private long endTime = 0;
    private long lTime  = 0;
    private long startTime = 0 ; 
	/**
	 * kMeans 군집 분석을 위한 메인 함수 
	 * @author Suhyun Jeon
	 * @author Moonie Song
	 * @param String[] args : 실행 인자 
	 * @date : 2013.07.02
	 * @version 0.0.1
	 */
	public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new KMeansDriver(), args);
        System.exit(res);
	}
	
	 /**
	  * GenericOptionsParser와 함께 작동하여 generic hadoop 명령 줄 인수를 구문 분석.<br>
	  * @param String[] args 실행 파라이터.(내부 실행용)
	  * @return int : 정상 수행시 0, 오류발생시 1
	  * @throws Exception
	  * @version 0.0.1
	  */
	@Override
	public int run(String[] args) throws Exception {

        logger.info("K-Means Clustering MR-Job is Started..");
		//하둡 환경 정보 로드.
		Configuration conf = this.getConf();
		//사용자가 입력한 알고리즘 변수를 파싱함.
		if(!ConfigurationVariable.setFromArguments(args, conf))
		{
            Usage.printUsage(Constants.DRIVER_KMEANS_CLUSTERING);
            logger.info("K-Means Clustering MR-Job is Failed..: Configuration Failed");
            return 1;
		}

		startTime = System.nanoTime();
        boolean isOnlyTest = false;
        boolean isTrained = false;
        boolean isTrainResultGen = false;   
		//군집 생성, 군집 할당 모드 선택.
        if(conf.get(ArgumentsConstants.TRAINED_MODEL, null) != null) isOnlyTest = true;

        String outputBase = conf.get(ArgumentsConstants.OUTPUT_PATH, null);
        String finalClusterPath = "";

        int iterCnt = 0;

        //새롭게 군집을 생성할 경우.
        if(!isOnlyTest)
        {
        	//데이터 정규화 사용 여부.
            if(conf.get(ArgumentsConstants.NORMALIZE, "false").equals("true"))
            {
                logger.info("Normalization for K-Means is Started..");
                String normalizationJobOutput = outputBase + "/normalize";
                
                //정규화를 위한 파라미터 로드.
                String params[] = getParametersForNormalization(conf, normalizationJobOutput);
                //수치 데이터 정규화 job실행.
                int res = ToolRunner.run(new NormalizeDriver(), params);
                if(res!=0)
                {
                	//job 수행 실패시 리턴함.
                    logger.info("Normalization for K-Means is Failed..");
                    return 1;
                }
                //정규화 결과 폴터를 군집 알고리즘의 입력으로 전환.
                logger.info("Normalization for K-Means is Successfully Finished..");
                conf.set(ArgumentsConstants.INPUT_PATH, normalizationJobOutput);
            }

            /**
             * clustering process
             * 1. set initial cluster center (old-cluster): Main Driver Class
             * 		numeric - distribution based ( min + ((max-min)/clusterCnt*clusterIndex))
             * 		nominal - frequency ratio
             * 2.assign cluster to each data and update cluster center (MR Job)
             * 		2.1 assign cluster to data using cluster-data distance (for all data): Map
             * 		2.2 update cluster center (new-cluster): Reduce
             * 4. compare old-cluster / new-cluster: Main Driver Class
             * 5. decision: Main Driver Class((
             * 		if(euqal) exec 2.1 and finish
             * 		else if(maxIter) exec 2.1 and finish
             * 		else
             * 		{
             * 			pre-cluster <= post-cluster
             * 			goto Step 2
             * 		}
             *
             * cluster representation
             * 0. id
             * 1. numeric => index '' value
             * 2. nominal => index '' value@@ratio[@@value@@ratio]+
             *
             * cluster-data distance
             * 1. numeric => clusterVal - dataVal
             * 2. nominal => 1 - ratio(cluster=data)
             * => Total Distance (Euclidean / Manhatan)
             */
            logger.info("Core Part for K-Means is Started..");

            // TODO: convergence rate
            mConvergeRate = Double.parseDouble(conf.get(ArgumentsConstants.CLUSTER_TRAINING_CONVERGE, mConvergeRate + ""));
            setIndexArray(conf);

            logger.info("> Cluster Center Initializing is Started....");
			/**
			 * @desc 초기 클러스터 설정(파일로 저장함)
			 *
			 * @parameter
			 *       conf        configuration identifier for job (non-mr)
			 *       clusterOutputPath       file path for cluster info
			 * @return
			 *  없음.
			 */
            String oldClusterPath = outputBase + "/cluster_center_0";
            String newClusterPath;
            setInitialClusterCenter(conf, oldClusterPath);							

            //조건을 만족할 때까지 클러스터 변경.
            logger.info("> Cluster Center Initializing is Finished....");
            logger.info("> Iteration(Cluster Assign / Cluster Update) is Started....");
            while(true)
            {
                iterCnt++; //클러스터 반복 횟수.
                logger.info("> Iteration: " + iterCnt);

                newClusterPath = outputBase + "/cluster_center_" + iterCnt;
                conf.set(ArgumentsConstants.OUTPUT_PATH, newClusterPath);
                logger.info(">> MR-Job for Cluster Assign / Cluster Update is Started..");
                
                if(!assignAndResetCluster(conf, oldClusterPath))
                {
                    logger.info(">> MR-Job for Cluster Assign / Cluster Update is Failed..");
                    return 1;			// MR Job, assign and update cluster
                }
                logger.info(">> MR-Job for Cluster Assign / Cluster Update is Finished...");
                logger.info(">> Iteration Break Condition Check..");

                if(isClustersEqual(conf, oldClusterPath, newClusterPath)) break;	// cluster check
                else if(iterCnt >= Integer.parseInt(conf.get(ArgumentsConstants.MAX_ITERATION, mIterMax + ""))) break;

                logger.info(">> Iteration is not Broken. Continue Next Iteration..");
                oldClusterPath = newClusterPath;
            }
            logger.info(">> Iteration is Broken..");
            logger.info("> Iteration(Cluster Assign / Cluster Update) is Finished....");

            isTrained = true;
            //객체에 클러스터 할당 여부.
            if(conf.get(ArgumentsConstants.FINAL_RESULT_GENERATION, "true").equals("true"))
                isTrainResultGen = true;

            finalClusterPath = newClusterPath;
            endTime = System.nanoTime();
    		lTime = endTime - startTime;
        }	
        //군집 종료.
        
        // testing (model adaptation), final assign
        boolean isOnlyTestNormalize = false;
        String clustering_result = "";
      
        if(isOnlyTest || isTrainResultGen)
        {
        	//입력 데이터의 거리만 산출할 경우.
            if(isTrainResultGen)
            {
                conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/clustering_result");
                //Mapper를 사용하여 객체에 클러스터 할당.
                if(!finalAssignCluster(conf, finalClusterPath)) return 1;					// Map Job, final assign cluster
                logger.info("> Finish Cluster Assign and Compute Distance for Trained Data..");
            }
            //기존 데이터의 거리에 세로운 데이터를 적용할 경우.
            else if(isOnlyTest)
            {
                String trainedModelBaseStr = conf.get(ArgumentsConstants.TRAINED_MODEL, null);
                String clusterCenterPath = "";

                FileSystem fs = FileSystem.get(conf);
                FileStatus[] status = fs.listStatus(new Path(trainedModelBaseStr));
                for (int i=0;i<status.length;i++)
                {
                    if(status[i].getPath().toString().contains("normalize_numericStat")) isOnlyTestNormalize = true;
                    if(status[i].getPath().toString().contains("cluster_center_"))
                    {
                        clusterCenterPath = status[i].getPath().toString();
                    }
                }

                if(isOnlyTestNormalize)
                {
                    String normalisedOutputPath = outputBase + "/normalize";
                    execNormalize(conf, trainedModelBaseStr, normalisedOutputPath);
                    conf.set(ArgumentsConstants.INPUT_PATH, normalisedOutputPath);
                }

                conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/clustering_result");
                conf.set(ArgumentsConstants.CLUSTER_PATH, clusterCenterPath);
                String modelInfoArr[] = getModelInfoStrArr(conf, clusterCenterPath);
                conf.set(ArgumentsConstants.TARGET_INDEX, modelInfoArr[0]);
                conf.set(ArgumentsConstants.NOMINAL_INDEX, modelInfoArr[1]);
                conf.set(ArgumentsConstants.CLUSTER_COUNT, modelInfoArr[2]);

                finalClusterPath = clusterCenterPath;
              //Mapper를 사용하여 객체에 클러스터 할당.
                if(!finalAssignCluster(conf, finalClusterPath)) return 1;					// Map Job, final assign cluster
                logger.info("> Finish Cluster Assign and Compute Distance for New Data..");

            }
            //set input path from output path
            conf.set(ArgumentsConstants.INPUT_PATH, conf.get(ArgumentsConstants.OUTPUT_PATH));
            System.out.println("Norm Inputpath : " + conf.get(ArgumentsConstants.INPUT_PATH));
            
            //set output path from modity
            conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/purity_mr"); 
            System.out.println("Norm OutputPath : " + conf.get(ArgumentsConstants.OUTPUT_PATH));
            
            int clusterIndex = ClusterCommon.getClusterIndex(conf);
            String params[] = ClusterCommon.getParametersForPurity(conf, clusterIndex);

            int res = ToolRunner.run(new NominalStatsDriver(), params);
            if(res!=0)
            {
                logger.info("Purity Computation (Nominal Stats) for K-Means is Failed..");
                return 1;
            }
            //Change output file for path and csv
            //ClusterCommon.finalPurityGen(conf, outputBase + "purity");
            ClusterCommon.finalPurityGen_csv(conf, outputBase + "/purity.csv");
            FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)), true);
            logger.info("Purity Computation (Nominal Stats) for K-Means is Successfully Finished..");
        }

        // temp delete
        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            logger.info("Temporary Files are Deleted..: Cluster Center Info Files");
            if(isTrained)
            {
                for(int i=0; i<iterCnt; i++)
                {
                    FileSystem.get(conf).delete(new Path(outputBase + "/cluster_center_" + i), true);
                }
            }
            if(conf.get(ArgumentsConstants.NORMALIZE, "false").equals("true") || isOnlyTestNormalize)
            {
                FileSystem.get(conf).delete(new Path(outputBase + "/normalize"), true);
            }
        }

        logger.info("Core Part for K-Means is Successfully Finished...");
        endTime = System.nanoTime();
		lTime = endTime - startTime;
		
		System.out.println("kMeans Clustering Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
		logger.info("kMeans Clustering Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
        
		return 0;
	}
	
	/**
	 * 군집 결과를 csv로 변환하여 저장한다.
	 * @param FileSystem fs : 파일 시스템 변수
	 * @param String inputPath : 군집 결과 경로 
	 * @param String filePrefix : 군집 결과 파일명 패턴
	 * @param String result_name_pattern : 변환할 파일 확장자
	 * @author Moonie
	 * @version 0.0.1
	 */
	@SuppressWarnings("unused")
	private void mFileIntegration(FileSystem fs , String inputPath, String filePrefix, String result_name_pattern)
    {
    	try
    	{
	    	FileStatus[] status = fs.listStatus(new Path(inputPath));	    	
	    	FSDataOutputStream fout = fs.create(new Path(inputPath + result_name_pattern), true);	    	
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));            
            HashMap<String, Integer> cluster_count  = new HashMap<String, Integer>();            
	        for(int i=0; i<status.length; i++)
	        {
	            Path fp = status[i].getPath();
	            
	            if(fp.getName().indexOf(filePrefix)<0) continue;
	
	            FSDataInputStream fin = fs.open(status[i].getPath());
	            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));	            
	            
	            String readStr, tokens[];
	            int value;
	            while((readStr=br.readLine())!=null)
	            {
	            	readStr = readStr.replace("\t", ",");
	            	System.out.println(readStr);
	            	String[] fields = readStr.split(",");
	            	String clusterID = fields[fields.length-2];
	            	if(cluster_count.containsKey(clusterID) == true)
	            	{
	            		int count = cluster_count.get(clusterID);
	            		cluster_count.put(clusterID, count+1);
	            	}
	            	else
	            	{
	            		cluster_count.put(clusterID, 1);
	            	}
	            	bw.write(readStr);
	            	bw.write("\r\n");
	            }
	            br.close();
	            fin.close();
	            //fs.delete(fp, true);
	        }
	        bw.close();
	        fout.close();
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.toString());
    	}
    }
    /**
     * 군집의 중심 정보를 배열로 반환한다.
     * @author Moonie
     * @param Configuration conf       : 하둡 시스템과 MapReduce 사이의 상호 작용 인자
     * @param String clusterCenterPath : 군집 파일 경로
     * @return String[] 군집 정보 배열
     * @throws Exception
     * @version 0.0.1
     */
    private String[] getModelInfoStrArr(Configuration conf, String clusterCenterPath) throws Exception
    {
        String returnArr[] = new String[3];
        for(int i=0; i<returnArr.length; i++) returnArr[i] = "";
        
        int clusterCnt = 0;
        String baseInfoStr = "";
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(clusterCenterPath));
        for (int i=0;i<status.length;i++)
        {
            Path fp = status[i].getPath();
            if(fp.getName().indexOf("part-") < 0) continue;
            FSDataInputStream fin = fs.open(fp);
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
            String readStr;
            while((readStr = br.readLine())!=null)
            {
                clusterCnt++;
                if(baseInfoStr.length() < 1) baseInfoStr = readStr;
            }
            br.close();
            fin.close();
        }
        returnArr[2] = clusterCnt + "";
        String tokens[] = baseInfoStr.split(conf.get(ArgumentsConstants.DELIMITER, "\t"));
        for(int i=1; i<tokens.length; i++)
        {
            if(tokens[i+1].contains(KMeansClusterInfoMgr.mNominalDelimiter)) 
            	returnArr[1] += "," + tokens[i];
            else returnArr[0] += "," + tokens[i];
            i++;
        }

        if(returnArr[0].length() == 0) returnArr[0] = "-1";
        else returnArr[0] = returnArr[0].substring(1);

        if(returnArr[1].length() == 0) returnArr[1] = "-1";
        else returnArr[1] = returnArr[1].substring(1);

        return returnArr;
    }
    
    /**
     * 수치 데이터 최대 최소 정규화 실행
     * @param Configuration conf         Job 설정 변수
     * @param String modelBasePathStr    최대, 최소 값이 저장된 경로
     * @param String outputPath          정규화된 값이 출력된 경로
     * @return 정상 종료시 0, 오류시 1 
     * @author Moonie
     * @version 0.0.1
     */
    private int execNormalize(Configuration conf, String modelBasePathStr, String outputPath) throws Exception
    {
        Job job = new Job(this.getConf());
        NormalizeDriver normal = new NormalizeDriver();

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        String targetIndexStr = normal.setMinMax(job.getConfiguration(), modelBasePathStr + "/normalize_numericStat");

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, targetIndexStr);
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, "-1");
        job.getConfiguration().set(ArgumentsConstants.REMAIN_FIELDS, "true");
        job.getConfiguration().set(ArgumentsConstants.TEMP_DELETE, "true");

        job.setJarByClass(KMeansDriver.class);
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

        return 0;
    }
    
    /**
     * 입력 데이터 셋을 이용하여 군집 초기 중심 정보 파일 생성
     * @param Configuration conf         Job 설정 변수
     * @param String clusterOutputPath   군집 출력 경로 
     * @author Moonie
     * @version 0.0.1
     */
    @Deprecated
	private void setInitialClusterCenter(Configuration conf, String clusterOutputPath) throws Exception
	{
		//하둡의 설정정보를 이용하여 파일 시스템 객제 생성.
		FileSystem fs = FileSystem.get(conf);
				
		String readStr, tokens[];
		int cluster_idx = 0;
		int clusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));
		KMeansClusterInfoMgr clusters[] = new KMeansClusterInfoMgr[clusterCnt];
		
		//HDFS의 데이터를 읽어들임.
        Path inputPath = new Path(conf.get(ArgumentsConstants.INPUT_PATH));
        inputPath = CommonMethods.findFile(fs, inputPath);
		FSDataInputStream fin = fs.open(inputPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
		//데이터 스캔 루프.
		while((readStr = br.readLine())!=null)
		{
			//클러스터 ID할당.
			clusters[cluster_idx] = new KMeansClusterInfoMgr();
			clusters[cluster_idx].setClusterID(cluster_idx);
			
			tokens = readStr.split(conf.get(ArgumentsConstants.DELIMITER, "\t"));
			//스캔한 레코드를 객체화  저장.
			for(int attr_idx = 0; attr_idx<tokens.length; attr_idx++)
			{
				if(!CommonMethods.isContainIndex(mExceptionIndexArr, attr_idx, false))
                {
                    if(CommonMethods.isContainIndex(mNominalIndexArr, attr_idx, false))
                    {
                    	clusters[cluster_idx].addAttributeValue(attr_idx, tokens[attr_idx], Constants.DATATYPE_NOMINAL);
                    }
                    else if(CommonMethods.isContainIndex(mIndexArr, attr_idx, false))
                    {
                    	clusters[cluster_idx].addAttributeValue(attr_idx, tokens[attr_idx], Constants.DATATYPE_NUMERIC);
                    }
                }
			}			
			//클래스터 갯수를 초과하지 않으면 데이터를 무작위 갯수(1~5)로 건너뜀.
			cluster_idx++;
			if(cluster_idx >= clusterCnt)
			{
				break;
			}
            else
            {
            	int skipCnt = (int) (Math.random() * 5);
                for(int i=0; i<skipCnt; i++)
            	{
                	br.readLine();
            	}
            }
		}
		br.close();
		fin.close();
        if(cluster_idx < clusterCnt)
        {
            logger.error("Initial Cluster Setting Error.");
            logger.error("> the number of initial clusters is less than the number of required clusters.");
            throw new Exception();
        }
        //임시 저장된 크기 클러스터 정보를 파일로 저장.
		FSDataOutputStream fout = fs.create(new Path(clusterOutputPath + "/part-r-00000"), true);		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
		for(int i=0; i<clusters.length; i++)
		{
			bw.write(clusters[i].getClusterInfoString(conf.get(ArgumentsConstants.DELIMITER, "\t")) + "\n");
		}
		bw.close();
		fout.close();
	}

    /**
     * MapReduce를 사용하여 군집의 중심 정보를 계산한다.
     * @param Configuration conf  	     Job 설정 변수
     * @param String oldClusterPath      이전 클러스터 정보.
     * @return 정상 종료시 true, 오류시 false 
     * @author Moonie
     * @version 0.0.1
     */
	private boolean assignAndResetCluster(Configuration conf, String oldClusterPath) throws Exception
	{
		/**
		 * <br>Mapper는 속성마다 데이터를 합한다.
	     * <br>Reducer는 합한 데이터의 평균을 산출한다.
		 * Map Job
		 * 		- load old cluster center
		 * 		- each data -> compute distance to each cluster
		 * 		- assign cluster number using distance: Map Key for Reduce
		 * 
		 * Reduce Job
		 * 		- compute new center
		 * 		- save key and computed center info
		 */		
		Job job = new Job(this.getConf());
		
		FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
		
		job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
		job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.NOMINAL_INDEX, conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
		job.getConfiguration().set(ArgumentsConstants.CLUSTER_COUNT, conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));		
		job.getConfiguration().set(ArgumentsConstants.CLUSTER_PATH, oldClusterPath);
        job.getConfiguration().set(ArgumentsConstants.DISTANCE_OPTION, conf.get(ArgumentsConstants.DISTANCE_OPTION, Constants.CORR_UCLIDEAN));
		job.setJarByClass(KMeansDriver.class);
		
		//분산된 데이터가 mapper함수로 들어감.
		job.setMapperClass(KMeansClusterAssignMapper.class);
		job.setReducerClass(KMeansClusterUpdateReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		if(!job.waitForCompletion(true))
    	{
        	logger.error("Error: MR for KMeans(Rutine) is not Completeion");
        	return false;
        }
		
        return true;
	}

   /**
    * 이전 군집과 새로운 군집의 중심이 같은지 비교 
    * @param Configuration conf  Job 설정 변수
    * @param String oldClusterPath      이전 클러스터 정보.
    * @param String newClusterPath      신규 클러스터 정보.
    * @return 정상 종료시 true, 오류시 false 
    * @author Moonie
    * @version 0.0.1
    */
	private boolean isClustersEqual(Configuration conf, String oldClusterPath, String newClusterPath) throws Exception
	{
		/**
		 * Check clusters are equal (cluster index and center info)
		 * Load 2 files
		 * 		HashMap Structure: Key - Cluster Index
		 * 							Value - Cluster Center Info. String
		 * for each Value of Keys, check
		 */
		
		int clusterCnt = Integer.parseInt(conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));
		String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
				
		KMeansClusterInfoMgr oldClusters[] = KMeansClusterInfoMgr.loadClusterInfoFile(conf, new Path(oldClusterPath), clusterCnt, delimiter);
		KMeansClusterInfoMgr newClusters[] = KMeansClusterInfoMgr.loadClusterInfoFile(conf, new Path(newClusterPath), clusterCnt, delimiter);
		
		for(int i=0; i<clusterCnt; i++)
		{
//            convergence rate check
			if(!oldClusters[i].isEqualClusterInfo(newClusters[i], mConvergeRate)) return false;
		}
		return true;
	}

	/**
    * 각 데이터 마다 최종 군집 번호를 할당한다.
    * @param Configuration conf  Job 설정 변수
    * @param String clusterPath  마지막 클러스터 정보 경로 
    * @return 정상 종료시 true, 오류시 false 
    * @author Moonie
    * @version 0.0.1
    */
	private boolean finalAssignCluster(Configuration conf, String clusterPath) throws Exception
	{
		/**
		 * Map Job (ref. Map job of 'assignAndResetCluster()')
		 * 
		 * If cat use MR default delimiter then, use Map job of 'assignAndResetCluster()'
		 * else, * Modified Map Job for Writing (no key, key is used to last attribute)
		 */
		Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.NOMINAL_INDEX, conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.CLUSTER_COUNT, conf.get(ArgumentsConstants.CLUSTER_COUNT, "1"));
        job.getConfiguration().set(ArgumentsConstants.CLUSTER_PATH, clusterPath);
        job.getConfiguration().set(ArgumentsConstants.DISTANCE_OPTION, conf.get(ArgumentsConstants.DISTANCE_OPTION, Constants.CORR_UCLIDEAN));
        job.setJarByClass(KMeansDriver.class);
		
		job.setMapperClass(KMeansClusterAssignFinalMapper.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);
		
		if(!job.waitForCompletion(true))
    	{
        	logger.error("Error: MR for KMeans(Final) is not Completion");
        	return false;
        }
		//clustering_result
		FileSystem fs = FileSystem.get(conf);
		String inputpath = conf.get(ArgumentsConstants.OUTPUT_PATH);
		mFileIntegration(fs, inputpath,"part-m",".csv");
        return true;
	}

   /**
    * 정규화를 위한 인자 설정.
    * @param Configuration conf  Job 설정 변수
    * @param String outputPath   정규화 결과 출력 경로 
    * @return String array[]     정규화 알고리즘 인자 배열
    * @author Moonie  
    * @version 0.0.1   
    */
	private String[] getParametersForNormalization(Configuration conf, String outputPath) throws Exception 
	{
		String params[] = new String[16];
		
		params[0] = ArgumentsConstants.INPUT_PATH;
		params[1] = conf.get(ArgumentsConstants.INPUT_PATH, null);
		
		params[2] = ArgumentsConstants.OUTPUT_PATH;
		params[3] = outputPath;
		
		params[4] = ArgumentsConstants.DELIMITER;
		params[5] = conf.get(ArgumentsConstants.DELIMITER, "\t");
		
		params[6] = ArgumentsConstants.TARGET_INDEX;
		params[7] = conf.get(ArgumentsConstants.TARGET_INDEX, "-1");
		
		String nominalIndexList = conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1");
		String exceptionIndexList = conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1");
		if(!nominalIndexList.equals("-1"))
		{
			if(exceptionIndexList.equals("-1")) exceptionIndexList = nominalIndexList;
			else exceptionIndexList += "," + nominalIndexList;
		}
		params[8] = ArgumentsConstants.EXCEPTION_INDEX;
		params[9] = exceptionIndexList;
		
		params[10] = ArgumentsConstants.REMAIN_FIELDS;
		params[11] = "true";
		
		// TODO: normalization modify
        params[12] = ArgumentsConstants.TEMP_DELETE;
		params[13] = conf.get(ArgumentsConstants.TEMP_DELETE, "true");
		
		params[14] = ArgumentsConstants.MR_JOB_STEP;
		params[15] = conf.get(ArgumentsConstants.MR_JOB_STEP, "1");
		
		return params;
	}

   /**
    * 콤마 기반 문자열 인덱스를 정수 배열로 변환함.
    * @param Configuration conf : Job 설정 변수
    * @author Moonie 
    * @version 0.0.1
    */
	private void setIndexArray(Configuration conf)
	{
		mIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		mNominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		mExceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
	}

	
}
