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


package org.ankus.mapreduce.algorithms.classification.MultilayerPerceptron;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ConfusionMatrixMapper;
import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ConfusionMatrixReducer;
import org.ankus.mapreduce.algorithms.classification.rmse.RMSEMapper;
import org.ankus.mapreduce.algorithms.classification.rmse.RMSEReducer;
import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ValidationMain;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
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
 * Multilayer Perceptron
 * @desc
 *
 * @version 0.8
 * @date : 2016.08.05
 * @author Song Sug Yeol
 */
public class MLPDriver extends Configured implements Tool {
/**
 * TODO: 20160708
 * - min, max, data type index list -> conf
 * - before nomalization, one line parse and define data type 
 */
	
    private Logger logger = LoggerFactory.getLogger(MLPDriver.class);
    
    private String m_Seed;
    private String m_NumHiddenNodes;
    private String m_LearningRate;
    private String m_Momentum;
    private String m_EndCondition;
    private String m_MaxEpoch;
    private String m_SubMaxEpoch;
    private String m_MaxError;
    private String m_NomalizeType = "0";
    private String m_classIdx;

    public int run(String[] args) throws Exception
    {
        
    	/**
    	 * TODO: 자동 밸류 인덱싱을 수동으로 교체. 자동은 살림 
    	 * 16-07-14
    	 */
        Configuration conf = this.getConf();
//        conf.set("fs.default.name",  "hdfs://localhost:9000");
        if(!ConfigurationVariable.setFromArguments(args, conf))
        {
            logger.info("> MR Job Setting Failed..");
            return 1;
        }
        
        conf.set("fs.hdfs.impl", 
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
            );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
            );

        boolean isOnlyTest = false;
        boolean isTrained = false;
        boolean isTrainResultGen = false;
        boolean isValidation = false;
        boolean isTestFileExsit = false;
        
       MLP_DEF model = null;
        
        if(conf.get(ArgumentsConstants.TRAINED_MODEL, null) != null){
        	isOnlyTest = true;
        	model = getModel(conf);
        } 

        String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        
        //Temp File Folder Name
        String errorPath = "result";
        String outputTempStr = "_nomalizeNum"; 
        String outputTempTestStr = "_nomalizeNumTest";
        String outputTempStrForNominal2 = "_nomalizeNominal";
		String mlpTmpFilePath1 = "_weight";
		String mlpTmpFilePath2 = "_weigthMerge";
		String nominalNomalizeFilePath = "nomalizeMerge.txt";
		String nominalNomalizeFilePath4Test = "nomalizeMerge4Test.txt";
		
		String minMaxFilePath = "minMaxMerge.txt";
		String minMaxFilePath4Test = "minMaxMerge4Test.txt";
	
		String outputTempStrForNominal1 = "_getNominalKey";
		String outputTempStrForNominal24Test = "_nomalizeNominal4Test";
		
		String numericKeyPath = "_numericNomalize";
		
		
		
		double[][] minmax = null;
		
		Path p = null;
		String fileName = null;
		File mf = null;
		
		if(isOnlyTest){
			
		} else {
			p = new Path(conf.get(ArgumentsConstants.INPUT_PATH));
			fileName = new String();
	        mf = new File(p.toString());
	        
	        if(!mf.isDirectory()){
	        	System.out.println(mf.getParent());
	        	p = new Path(mf.getParent()); //main input directory for tmpFolder
	        	fileName = mf.getName();
	        }
		}
        
        
        
		
        //preprocessing
        m_Seed = conf.get(ArgumentsConstants.SEED);
        m_NumHiddenNodes = conf.get(ArgumentsConstants.HIDDEN_NODE_NUM);
        m_LearningRate = conf.get(ArgumentsConstants.LEARNING_RATE);
        m_Momentum = conf.get(ArgumentsConstants.MOMENTUN);
        m_EndCondition = conf.get(ArgumentsConstants.END_CONDITION);
        m_MaxEpoch = conf.get(ArgumentsConstants.MAX_EPOCH);
        m_SubMaxEpoch = conf.get(ArgumentsConstants.SUB_MAX_EPOCH);
        m_MaxError = conf.get(ArgumentsConstants.MAX_ERROR);
        
        
        String nomList;
        if(conf.get(ArgumentsConstants.NOMINAL_INDEX) != null){
        	nomList = conf.get(ArgumentsConstants.NOMINAL_INDEX).toString();
        } else {
        	nomList = "";
        }
        String numList;
        
        if(conf.get(ArgumentsConstants.NUMERIC_INDEX) == null){
        	numList = "";
        } else {
        	numList = conf.get(ArgumentsConstants.NUMERIC_INDEX).toString();
        }
         
        String targetList;
        
        if(nomList.length() > 0 ){
        	targetList = nomList;
        }
        
        if(nomList.length() > 0 && numList.length() > 0) {
        	targetList = nomList + "," + numList;
        } else {
        	targetList = numList;
        }
        
//        System.out.println(targetList);
        
        conf.set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, targetList));
        
//        String[] targetIndx = conf.getPropertySources(ArgumentsConstants.TARGET_INDEX).toString().split(",");
        
        m_NomalizeType = "0";
        m_classIdx = conf.get(ArgumentsConstants.CLASS_INDEX);
        
        conf.set(ArgumentsConstants.OUTPUT_PATH, conf.get(ArgumentsConstants.OUTPUT_PATH)+"/");
        
        if(isOnlyTest) {
        	
        } else {
        	  conf.set(ArgumentsConstants.SEED, conf.get(ArgumentsConstants.SEED, m_Seed));
              conf.set(ArgumentsConstants.HIDDEN_NODE_NUM, conf.get(ArgumentsConstants.HIDDEN_NODE_NUM, m_NumHiddenNodes));
              conf.set(ArgumentsConstants.LEARNING_RATE, conf.get(ArgumentsConstants.LEARNING_RATE, m_LearningRate));
              conf.set(ArgumentsConstants.MOMENTUN, conf.get(ArgumentsConstants.MOMENTUN, m_Momentum));
//              conf.set(ArgumentsConstants.END_CONDITION, conf.get(ArgumentsConstants.END_CONDITION, m_EndCondition));
              conf.set(ArgumentsConstants.MAX_EPOCH, conf.get(ArgumentsConstants.MAX_EPOCH, m_MaxEpoch));
//              conf.set(ArgumentsConstants.SUB_MAX_EPOCH, conf.get(ArgumentsConstants.SUB_MAX_EPOCH, m_SubMaxEpoch));
        }
      
        conf.set(ArgumentsConstants.NOMALIZE_TYPE, conf.get(ArgumentsConstants.NOMALIZE_TYPE, m_NomalizeType));
        conf.set(ArgumentsConstants.CLASS_INDEX, conf.get(ArgumentsConstants.CLASS_INDEX, m_classIdx));
        
        conf.set(ArgumentsConstants.DELIMITER, delimiter);
        
        if(conf.get(ArgumentsConstants.TEST_DATA) != null){
        	isTestFileExsit = true;
        }
        
//       System.out.println(isTestFileExsit);
        //class Data Type Detect
//        String typeIdx = getNominalAttrs(conf, conf.get(ArgumentsConstants.INPUT_PATH));
       String typeIdx = getNominalAttrs(conf.get(ArgumentsConstants.NOMINAL_INDEX),  conf.get(ArgumentsConstants.NUMERIC_INDEX));
//       System.out.println(typeIdx);
//       System.out.println(conf.get(ArgumentsConstants.NOMINAL_INDEX));
        
        String classIdx = conf.get(ArgumentsConstants.CLASS_INDEX);
        String mergeFilePath;
        String mergeFilePathTest;
        
        if(conf.get(ArgumentsConstants.NUMERIC_INDEX) != null){
	        for(int i = 0; i < conf.get(ArgumentsConstants.NUMERIC_INDEX).split(",").length; i++){
	        	if(conf.get(ArgumentsConstants.NUMERIC_INDEX).split(",")[i].equals(classIdx)){
	        		conf.set(ArgumentsConstants.CLASS_INDEX_TYPE, conf.get(ArgumentsConstants.CLASS_INDEX_TYPE, "0"));
	        	}
	        }
		}
        
        if(conf.get(ArgumentsConstants.CLASS_INDEX_TYPE) == null){
        	conf.set(ArgumentsConstants.CLASS_INDEX_TYPE, conf.get(ArgumentsConstants.CLASS_INDEX_TYPE, "1"));
        }
//        System.out.println("체크"+conf.get(ArgumentsConstants.CLASS_INDEX_TYPE));
//        
//        if(classIdx.split(",").length > 1){
//        	conf.set(ArgumentsConstants.CLASS_INDEX_TYPE, conf.get(ArgumentsConstants.CLASS_INDEX_TYPE, "0"));
//        	//0 = numeric
//        } else {
//        	if(typeIdx.split("\t")[Integer.parseInt(classIdx)].equals("1")){
//        		//nominal
//        		conf.set(ArgumentsConstants.CLASS_INDEX_TYPE, conf.get(ArgumentsConstants.CLASS_INDEX_TYPE, "1"));
//        	} else {
//        		//numeric
//        		conf.set(ArgumentsConstants.CLASS_INDEX_TYPE, conf.get(ArgumentsConstants.CLASS_INDEX_TYPE, "0"));
//        	}
//        }
         
        logger.info("Random Seed: " + conf.get(ArgumentsConstants.SEED));
        logger.info("Hidden Nodes Size: " + conf.get(ArgumentsConstants.HIDDEN_NODE_NUM));
        logger.info("Learning Rate: " + conf.get(ArgumentsConstants.LEARNING_RATE));
        logger.info("Momentum: " + conf.get(ArgumentsConstants.MOMENTUN));
        logger.info("Main Max Epoch: " + conf.get(ArgumentsConstants.MAX_EPOCH));
        logger.info("Slave Max Epoch: " + conf.get(ArgumentsConstants.LEARNING_RATE));
        logger.info("Nominal Attribute Index: " + conf.get(ArgumentsConstants.NOMINAL_INDEX));
        logger.info("Numeric Attribute Index: " + conf.get(ArgumentsConstants.NUMERIC_INDEX));
        logger.info("> MultilayerPerceptron Classification Iterations (Training) are Started..");
        logger.info("> : Training Data Nomalization is Started..");
//                        
        logger.info("Previous Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH)+ outputTempStr);
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+ outputTempStr), true);
    	logger.info("Previous Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH)+ outputTempStrForNominal2);
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+ outputTempStrForNominal2), true);
    	logger.info("Previous Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH)+ mlpTmpFilePath1);
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+ mlpTmpFilePath1), true);
    	logger.info("Previous Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH)+ mlpTmpFilePath2);
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+ mlpTmpFilePath2), true);
    	logger.info("Previous Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH)+ nominalNomalizeFilePath);
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+ nominalNomalizeFilePath), true);
    	logger.info("Previous Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH)+ minMaxFilePath);
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+ minMaxFilePath), true);
    	logger.info("Previous Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH)+ outputTempStrForNominal1);
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+ outputTempStrForNominal1), true);
    	logger.info("Previous Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH)+ numericKeyPath);
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+ numericKeyPath), true);
    	
    	logger.info("Previous Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH) +"validation_tmp");
    	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) +"validation_tmp"), true);
    	
    	StringBuffer minBuf = new StringBuffer();
        StringBuffer maxBuf = new StringBuffer();
        
        if(isOnlyTest){
        	minBuf = model.minBuf;
        	maxBuf = model.maxBuf;
        }
        
        StringBuffer mixMaxListBuf = new StringBuffer();
    	
        if(!isOnlyTest && conf.get(ArgumentsConstants.NUMERIC_INDEX) != null){        
	        @SuppressWarnings("deprecation")
	        Job job = new Job(this.getConf());
	        
	        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
	        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
	        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
	        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
	        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
	
	        job.setJarByClass(MLPDriver.class);
	        
	        job.setMapperClass(MLP_NomalizeNumericMapper1.class);
			job.setReducerClass(MLP_NomalizeNumericReducer1.class);
	
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
	
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
	                			
	        if(!job.waitForCompletion(true))
	        {
	        	logger.info("Error: (Job1)Preprocessing for MultilayerPerceptron is not Completeion");
	            return 1;
	        }
	           
	        minmax = getMinMaxCombine(conf, conf.get(ArgumentsConstants.OUTPUT_PATH));
	        System.out.println("======="+minmax);
	        logger.info("MR-Job for check Min/Max successfully finished..");
	        logger.info("MR-Job for Min/Max Nomlaization is Started....");
	            
	        
	            
	        for(int i = 0 ; i < minmax.length; i++){
	         	minBuf.append(minmax[i][2]);
	           	minBuf.append(",");
	           	
	           	maxBuf.append(minmax[i][1]);
	           	maxBuf.append(",");
	            	
	           	mixMaxListBuf.append(i);
	           	mixMaxListBuf.append(",");
	           	
	           	if(i == Integer.parseInt(m_classIdx)){
	           		conf.set(ArgumentsConstants.CLASS_MAX, new String().valueOf(minmax[i][1]));
	           		conf.set(ArgumentsConstants.CLASS_MIN, new String().valueOf(minmax[i][2]));
	           	}
	        }
	           
	//           System.out.println(typeIdx);
	        @SuppressWarnings("deprecation")
			Job job1 = new Job(this.getConf());
	        job1.setJarByClass(MLPDriver.class);       
	        set2StepJob1(job1, conf, p, outputTempStr, minBuf.toString(), maxBuf.toString(), mixMaxListBuf.toString());
	          
	        job1.setMapperClass(MLP_NomalizeNumericMapper2.class);
			job1.setReducerClass(MLP_NomalizeNumericReducer2.class);
	
			job1.setMapOutputKeyClass(IntWritable.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setOutputKeyClass(NullWritable.class);
			job1.setOutputValueClass(Text.class);
				
			if(!job1.waitForCompletion(true))
	        {
				logger.info("Error: (Job2)Preprocessing for MultilayerPerceptron is not Completeion");
	            return 1;
	        }
			mergeFilePath = jobFileMerge(conf,p, outputTempStr, minMaxFilePath);
//			System.out.println(outputTempStr);
//			mergeFilePath = conf.get(ArgumentsConstants.OUTPUT_PATH) + "/"+outputTempStr;
        } else {
        	mergeFilePath = conf.get(ArgumentsConstants.INPUT_PATH);
        }
        String[] keyList = null;
        
        if(isOnlyTest) {
        	keyList = model.keyList;
        }
        
        String mergeFilePath2;
        String mergeFilePath24Test = null;
        
        
        if(!isOnlyTest && conf.get(ArgumentsConstants.NOMINAL_INDEX) != null){  
        	@SuppressWarnings("deprecation")
    		Job job2 = new Job(this.getConf());	
    		job2.setJarByClass(MLPDriver.class);  
    		set2StepJob2(job2, conf, p, mergeFilePath, outputTempStrForNominal1, typeIdx);
            job2.setMapperClass(MLP_NomalizeNominalMapper1.class);
    		job2.setReducerClass(MLP_NomalizeNominalReducer1.class);

    		job2.setMapOutputKeyClass(IntWritable.class);
    		job2.setMapOutputValueClass(Text.class);

    		job2.setOutputKeyClass(NullWritable.class);
    		job2.setOutputValueClass(Text.class);
    			
    		if(!job2.waitForCompletion(true))
            {
    			logger.info("Error: (Job3)Preprocessing for MultilayerPerceptron is not Completeion");
                return 1;
            }
    		keyList = job2Combine(conf, p, outputTempStrForNominal1);
    			
    		@SuppressWarnings("deprecation")
    		Job job3 = new Job(this.getConf());
    		job3.setJarByClass(MLPDriver.class);  

    		set2StepJob3(job3, conf, p, mergeFilePath , outputTempStrForNominal2, keyList);

    		job3.setMapperClass(MLP_NomalizeNominalMapper2.class);
    		job3.setReducerClass(MLP_NomalizeNominalReducer2.class);

    		job3.setMapOutputKeyClass(IntWritable.class);
    		job3.setMapOutputValueClass(Text.class);

    		job3.setOutputKeyClass(NullWritable.class);
    		job3.setOutputValueClass(Text.class);
    			    			
    		if(!job3.waitForCompletion(true))
            {
                logger.info("Error: (Job4)Preprocessing for MultilayerPerceptron is not Completeion");
                return 1;
            } else {
            	logger.info("> Preprocessing for MultilayerPerceptron is Completeion");
            }
    		mergeFilePath2 = jobFileMerge(conf,p,outputTempStrForNominal2, nominalNomalizeFilePath);
        } else {
        	mergeFilePath2 = mergeFilePath;
        }
 
		int numInputNode = 0;
        // training process
		
		
		if(isTestFileExsit){
			if(conf.get(ArgumentsConstants.NUMERIC_INDEX) != null){ 
				@SuppressWarnings("deprecation")
				Job job14Test = new Job(this.getConf());
				job14Test.setJarByClass(MLPDriver.class);       
				set2StepJob14Test(job14Test, conf, p, outputTempTestStr, minBuf.toString(), maxBuf.toString());
		          
		        job14Test.setMapperClass(MLP_NomalizeNumericMapper2.class);
		        job14Test.setReducerClass(MLP_NomalizeNumericReducer2.class);
		
		        job14Test.setMapOutputKeyClass(IntWritable.class);
		        job14Test.setMapOutputValueClass(Text.class);
		        job14Test.setOutputKeyClass(NullWritable.class);
		        job14Test.setOutputValueClass(Text.class);
					
				if(!job14Test.waitForCompletion(true))
		        {
					logger.info("Error: (Job2)Preprocessing for MultilayerPerceptron is not Completeion");
		            return 1;
		        } else {
		        	logger.info("Test Data Preprocessing Step 1 Complete");
		        }
				mergeFilePathTest = jobFileMerge(conf, p, outputTempTestStr, minMaxFilePath4Test);
	//			System.out.println(outputTempStr);
	//			mergeFilePath = conf.get(ArgumentsConstants.OUTPUT_PATH) + "/"+outputTempStr;
	        } else {
	        	mergeFilePathTest = conf.get(ArgumentsConstants.TEST_DATA);
	        }
			
			//Test Data Preproceesing Step 2
			
	        if(conf.get(ArgumentsConstants.NOMINAL_INDEX) != null){  
	    			
	    		@SuppressWarnings("deprecation")
	    		Job job34Test = new Job(this.getConf());
	    		job34Test.setJarByClass(MLPDriver.class);  

	    		set2StepJob3(job34Test, conf, p, mergeFilePathTest , outputTempStrForNominal24Test, keyList);

	    		job34Test.setMapperClass(MLP_NomalizeNominalMapper2.class);
	    		job34Test.setReducerClass(MLP_NomalizeNominalReducer2.class);

	    		job34Test.setMapOutputKeyClass(IntWritable.class);
	    		job34Test.setMapOutputValueClass(Text.class);

	    		job34Test.setOutputKeyClass(NullWritable.class);
	    		job34Test.setOutputValueClass(Text.class);
	    			    			
	    		if(!job34Test.waitForCompletion(true))
	            {
	                logger.info("Error: (Job4)Preprocessing for MultilayerPerceptron is not Completeion");
	                return 1;
	            } else {
	            	logger.info("> Preprocessing for TestFile 2 is Completeion");
	            }
	    		mergeFilePath24Test = jobFileMerge(conf,p,outputTempStrForNominal24Test, nominalNomalizeFilePath4Test);
	        } else {
	        	mergeFilePath24Test = mergeFilePathTest;
	        }
			
		}
		
		//트레이닝 + 테스트 파일 입력시 구현 16.11.03
		String wHidden = new String();
		String wOutput = new String();
		int numOutputNode = 0;
		
		//minBuf
		
        if(minBuf.toString() != null){
        	if(conf.get(ArgumentsConstants.CLASS_INDEX_TYPE) == "0"){
        		numInputNode = minBuf.toString().split(",").length-1;
        	} else {
        		numInputNode = minBuf.toString().split(",").length;
        	}
        } 
        
        if( keyList != null){
        	numInputNode += keyList[0].split(",").length;
        	numOutputNode = keyList[1].split(",").length;
        } else {
        	numOutputNode = conf.get(ArgumentsConstants.CLASS_INDEX).split(",").length;
        }
        
		this.numInputNodes = numInputNode;
		this.numHiddenNodes = Integer.parseInt(m_NumHiddenNodes);
		this.numOutputNodes = numOutputNode;
		
        if(!isOnlyTest)
        {
            if(conf.get(ArgumentsConstants.CLASS_INDEX, "-1").equals("-1"))
            {
                logger.info("> Class Index is must defined for Training in MultilayerPerceptron(Ankus).");
                return 1;
            }

            /**
    		 *Only Test mode
    		 */
            
    		int cntEpoch = 0;
    		int numEpoch = Integer.parseInt(conf.get(ArgumentsConstants.MAX_EPOCH));
    			
    		Random randSeed = new Random(Integer.parseInt(conf.get(ArgumentsConstants.SEED)));
    		
    		wHidden = generateWeigth(randSeed, numInputNode, Integer.parseInt(m_NumHiddenNodes));
    		wOutput = generateWeigth(randSeed, Integer.parseInt(m_NumHiddenNodes), numOutputNode);
    		
    		long sTime = System.currentTimeMillis();
    	    		
    		do{
    			@SuppressWarnings("deprecation")
				Job mlpJob = new Job(this.getConf());
    			mlpJob.setJarByClass(MLPDriver.class); 
	    		set2StepMLPJob(mlpJob, conf, mergeFilePath2, mlpTmpFilePath1, keyList, numInputNode, numOutputNode, wHidden, wOutput);
	    		mlpJob.setMapperClass(MLP_Mapper.class);
	    		mlpJob.setReducerClass(MLP_Reducer.class);
		
	    		mlpJob.setMapOutputKeyClass(IntWritable.class);
	    		mlpJob.setMapOutputValueClass(Text.class);
		
	    		mlpJob.setOutputKeyClass(NullWritable.class);
	    		mlpJob.setOutputValueClass(Text.class);
					    			
	    		if(!mlpJob.waitForCompletion(true))
		        {
		            logger.info("Error: Bulding MultilayerPerceptron is not Completeion");
		            return 1;
		        } else {
		            logger.info("> Bulding MultilayerPerceptron is Completeion");
		        }
					
				String mlpWeightPath = jobMLPWeightCombine(conf, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)) +"/"+ mlpTmpFilePath1, mlpTmpFilePath2 );
				String[] weightList = mergeWeight(conf, mlpWeightPath, numInputNode, Integer.parseInt(m_NumHiddenNodes), numOutputNode);
					
				wHidden = weightList[0];
				wOutput = weightList[1];
					
				this.g_wHidden = weightList[0];
				this.g_wOutput = weightList[1];
					
				buildNN();									
					
				logger.info("Temporary Files are Deleted..: " + conf.get(ArgumentsConstants.OUTPUT_PATH) + mlpTmpFilePath1);
    	       	FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)  + mlpTmpFilePath1), true);
				
    	       	cntEpoch++;
    		} while(  cntEpoch < numEpoch);
    		
    		
    		logger.info("> Trainging Time: "+ (System.currentTimeMillis() - sTime));
    		logger.info("> Training MultilayerPerceptron Model is Completeion");
    		
    		//여기서부터 테스트 데이터 전처리 부터....
    		
    		
    		outputModel(conf, keyList, minBuf, maxBuf,	wHidden,	wOutput);
//    		getModel()
    		//Finish Training
        } else {
        	//only Testing
        	logger.info("> Only Testing Mode");
        	
        	this.numInputNodes = numInputNode;
    		this.numHiddenNodes = Integer.parseInt(m_NumHiddenNodes);
    		this.numOutputNodes = numOutputNode;
        	
//    		String[] weightList = mergeWeight(conf, conf.get(ArgumentsConstants.TRAINED_MODEL), numInputNode, Integer.parseInt(m_NumHiddenNodes), numOutputNode);
        	//weigth file input
    		
    		wHidden = model.wHidden;
			wOutput = model.wOutput;
				
			this.g_wHidden = wHidden;
			this.g_wOutput = wOutput;
				
//			System.out.println(this.g_wHidden);
			buildNN();
        }    
        
        String testPath;
		
		if(isTestFileExsit){
			testPath = mergeFilePath24Test;
		} else {
			testPath = mergeFilePath2;
		}
//		System.out.println(mergeFilePath2);
//		System.out.println(mergeFilePath24Test);

		@SuppressWarnings("deprecation")
		Job testMlpJob= new Job(this.getConf());
		testMlpJob.setJarByClass(MLPDriver.class); 
		set2StepMLPJob(testMlpJob, conf, testPath, errorPath, keyList, numInputNode, numOutputNode, wHidden, wOutput);
		testMlpJob.setMapperClass(MLP_TestMapper.class);
		testMlpJob.setReducerClass(MLP_TestReducer.class);

		testMlpJob.setMapOutputKeyClass(IntWritable.class);
		testMlpJob.setMapOutputValueClass(Text.class);

		testMlpJob.setOutputKeyClass(NullWritable.class);
		testMlpJob.setOutputValueClass(Text.class);
			    			
		if(!testMlpJob.waitForCompletion(true))
        {
            logger.info("Error: Testing MultilayerPerceptron Model is not Completeion");
            return 1;
        }else {
        	logger.info("> Testing MultilayerPerceptron Model is Completeion");
        }
       
        // temp file delete process
        
        Path tmpP = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH));

//      Class type is Numeric -> RMSE, Nominal -> confusionMatrixGen
        if(conf.get(ArgumentsConstants.CLASS_INDEX_TYPE) == "0"){
        	System.out.println(errorPath);
        	rmseGen(conf, p, errorPath, numInputNode);
        } else if(conf.get(ArgumentsConstants.CLASS_INDEX_TYPE) == "1") {
        	confusionMatrixGen(conf, p, errorPath, numInputNode);
        }
        
//		logger.info("Temporary Files are Deleted..: " + tmpP+"/"+ outputTempStr);
//        FileSystem.get(conf).delete(new Path(tmpP+"/"+ outputTempStr), true);
//        logger.info("Temporary Files are Deleted..: " + tmpP+"/"+ outputTempStrForNominal2);
//        FileSystem.get(conf).delete(new Path(tmpP+"/"+ outputTempStrForNominal2), true);
//        logger.info("Temporary Files are Deleted..: " + tmpP+"/"+ mlpTmpFilePath1);
//        FileSystem.get(conf).delete(new Path(tmpP+"/"+ mlpTmpFilePath1), true);
////        logger.info("Temporary Files are Deleted..: " + tmpP+"/"+ mlpTmpFilePath2);
////        FileSystem.get(conf).delete(new Path(tmpP+"/"+ mlpTmpFilePath2), true);
//        logger.info("Temporary Files are Deleted..: " + tmpP+"/"+ nominalNomalizeFilePath);
//        FileSystem.get(conf).delete(new Path(tmpP+"/"+ nominalNomalizeFilePath), true);
//        logger.info("Temporary Files are Deleted..: " + tmpP+"/"+ minMaxFilePath);
//        FileSystem.get(conf).delete(new Path(tmpP+"/"+ minMaxFilePath), true);
//        logger.info("Temporary Files are Deleted..: " + tmpP+"/"+ outputTempStrForNominal1);
//        FileSystem.get(conf).delete(new Path(tmpP+"/"+ outputTempStrForNominal1), true);
//        logger.info("Temporary Files are Deleted..: " + tmpP+"/"+ numericKeyPath);
//        FileSystem.get(conf).delete(new Path(tmpP+"/"+ numericKeyPath), true);
//        logger.info("> Temporary Files are Deleted..");       
        
        
//        getModel(conf.get(ArgumentsConstants.TRAINED_MODEL));
        return 0;
    }
    
    private String getNominalAttrs(String nominalIndex, String numericIndex) {
		// TODO Auto-generated method stub
    	int len = 0;
    	if(nominalIndex == null){
    		len = numericIndex.split(",").length;
    	} else if( numericIndex == null){
    		len = nominalIndex.split(",").length;
    	} else {
    		len = numericIndex.split(",").length + nominalIndex.split(",").length;
    	}
 
    	String[] nominalList;
    	
    	if(nominalIndex != null && nominalIndex.length() > 0){
    		nominalList = nominalIndex.split(",");
    	} else {
    		nominalList = new String[0];
    	}

    	StringBuffer sb = new StringBuffer();
    	
    	boolean t = false;
    	for(int i = 0; i < len; i++){
    		t = false;
    		for(int j = 0 ; j < nominalList.length; j++){
    			if(i == Integer.parseInt(nominalList[j])){
    				sb.append("1");
    				t = true;
    				break;
    			}
    		}
    		
    		if(!t){
    			sb.append("0");
    		}
    		
    		if( i < len - 1){
    			sb.append("\t");
    		}
    	}
    	
		return sb.toString();
	}

	/**
     * 분산 MLP로 학습된 가중치파일을 하나의 가중치 파일로 합치는 함수  
     * @param conf
     * @param inputPath 클러스터에서의 파일 생성 경로 
     * @param outputFile 합쳐질 파일명 
     * @return
     */
    private String jobMLPWeightCombine(Configuration conf, String inputPath ,String outputFile){
	 	   	 
  
	   	try {
				FileSystem fs = FileSystem.get(conf);
				FSDataOutputStream fout = fs.create(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) +"/"+ outputFile), true);
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
				
				
				FileStatus[] status = fs.listStatus(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) +"/"+new Path(inputPath).getName()));
		    	for (int i=0;i<status.length;i++)
		        {
		            Path fp = status[i].getPath();
	
		            if(fp.getName().indexOf("part-")==0)
		            {
		                FSDataInputStream fin = fs.open(fp);
		                BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
	
		                String readStr;
		                while((readStr=br.readLine())!=null)
		                {
//		                	System.out.println(readStr);
		                	bw.write(readStr+"\r\n");
		                }
	
		                br.close();
		                fin.close();
		            }
		        }
		    	bw.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//	   	System.out.println(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+outputFile).toString());
	   	return new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+outputFile).toString();
   }
	public void outputModel(Configuration conf, String[] keyList, StringBuffer minBuf, StringBuffer maxBuf,	String wHidden,	String wOutput){
		System.out.println("Save Model");
//		FileSystem fs = FileSystem.get(conf);
//		FSDataOutputStream fout = fs.create(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/_numericNomalize/"+ outputFile), true);
		try{   
			System.out.print("경로\t");
            System.out.println(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/"+"net.out");
            FileSystem fs = FileSystem.get(conf);
            FSDataOutputStream fos =
            		fs.create(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/"+"net.out"), true); 
            System.out.print("확인\t");
            ObjectOutputStream oos =
                                 new ObjectOutputStream(fos); 
            oos.writeObject(new MLP_DEF(keyList, minBuf, maxBuf, wHidden, wOutput)); 
            oos.flush(); 
            fos.close(); 
        } 
        catch(Throwable e)  
        { 
            System.err.println(e); 
        }    
	}
	
	public MLP_DEF getModel(Configuration conf){
		MLP_DEF obj = null; 
		
	       try 
	       { 
	    	   FileSystem fs = FileSystem.get(conf);
	    	   FSDataInputStream fis =
	    			   fs.open(new Path(conf.get(ArgumentsConstants.TRAINED_MODEL))); 
	           ObjectInputStream ois =
	                                  new ObjectInputStream(fis); 
	           obj = (MLP_DEF)ois.readObject(); 
	           fis.close(); 
	       } 
	       catch(Throwable e) 
	       { 
	           System.err.println(e); 
	       } 
	        
	       System.out.println(obj.minBuf.toString()); 
	       System.out.println(obj.maxBuf.toString()); 
	       
	       System.out.println(obj.wHidden);
	       System.out.println(obj.wOutput);

	       return obj;
	}
    /**
     * 하나의 파일로 합쳐진 가중치파일을 읽어 평균 가중치를 구하는 함수 
     * @param conf
     * @param path
     * @param numInput
     * @param numHidden
     * @param numOutput
     * @return
     * @throws IOException
     */
    private String[] mergeWeight(Configuration conf, String path, int numInput, int numHidden, int numOutput) throws IOException{
    	FileSystem fs = FileSystem.get(conf);
    	FSDataInputStream fin = fs.open(new Path(path));
    	BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
    	
    	String readStr;
        double sumHiddenWeight[][] = new double[numHidden][numInput+1];
        double sumOutputWeight[][] = new double[numOutput][numHidden+1];

        int numCluster = 0;
        while((readStr=br.readLine())!=null)
        {
        	String wList[] = readStr.split("\t");
    		int len = wList.length;
    		
    		for ( int i = 1; i < len; i++){
    			String wNodeList[] = wList[i].split(",");
    			int subLen = wNodeList.length;
    			if(wList[0].contains("h")){
    				for(int j = 0; j < subLen ; j++){        				
        				sumHiddenWeight[i-1][j] += Double.parseDouble(wNodeList[j]);
        			}
    			} else if (wList[0].contains("o")){
    				for(int j = 0; j < subLen ; j++){      				
    					sumOutputWeight[i-1][j] += Double.parseDouble(wNodeList[j]);
        			}
    			}
    		}
    		numCluster++;
        }
        numCluster /= 2;
        String retList[] = new String[2];
        StringBuffer retBuf = new StringBuffer();
    	
    	for(int i = 0; i < sumHiddenWeight.length; i++){
    		for(int j = 0; j < sumHiddenWeight[0].length; j++){
    			retBuf.append(sumHiddenWeight[i][j]/numCluster);
    			
    			if( j < sumHiddenWeight[0].length-1){
    				retBuf.append(",");
    			}
    		}
    		if( i < sumHiddenWeight.length-1){
				retBuf.append("\t");
			}
    	}
    	retList[0] = retBuf.toString();
    	retBuf = new StringBuffer();
    	
    	for(int i = 0; i < sumOutputWeight.length; i++){
    		for(int j = 0; j < sumOutputWeight[0].length; j++){
    			retBuf.append(sumOutputWeight[i][j]/numCluster);
    			
    			if( j < sumOutputWeight[0].length-1){
    				retBuf.append(",");
    			}
    		}
    		if( i < sumOutputWeight.length-1){
				retBuf.append("\t");
			}
    	}
    	retList[1] = retBuf.toString();
    	
    	return retList;
    }
    
    /**
     * 신경망 초기 생성시 가중치를 무작위수치로 생성하는 함수 
     * @param rSeed
     * @param numInput
     * @param numOutput
     * @return
     */
    private String generateWeigth(Random rSeed, int numInput, int numOutput){
    	StringBuffer retBuf = new StringBuffer();
    	
    	double[][] weight = new double[numOutput][numInput+1];
    	
    	for(int i = 0; i < numOutput; i++){
    		for(int j = 0; j < numInput+1; j++){
    			weight[i][j] = rSeed.nextDouble() - 0.5f;
    		}
    	}
    	
    	for(int i = 0; i < numOutput; i++){
    		for(int j = 0; j < numInput+1; j++){
    			retBuf.append(weight[i][j]);
    			
    			if( j < numInput){
    				retBuf.append(",");
    			}
    		}
    		if( i < numOutput){
				retBuf.append("\t");
			}
    	}
    	return retBuf.toString();
    }
    
    private void set2StepJob3(Job job, Configuration conf, Path p, String inputPath,  String outputpathStr, String[] keyList) throws IOException{
		// TODO Auto-generated method stub
    	FileInputFormat.addInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+ outputpathStr));
        if(keyList[0] != null)
        	job.getConfiguration().set(ArgumentsConstants.NORMAL_NOMINAL_KEY_LIST, conf.get(ArgumentsConstants.NORMAL_NOMINAL_KEY_LIST, keyList[0]));
        job.getConfiguration().set(ArgumentsConstants.CLASS_NOMINAL_KEY_LIST, conf.get(ArgumentsConstants.CLASS_NOMINAL_KEY_LIST, keyList[1]));
	}
    
    private void set2StepMLPJob(Job job, Configuration conf, String inputPath, String outputpathStr, String[] keyList, int numInputNode, int numOutputNode, String hWeight, String oWeight) throws IOException{
		// TODO Auto-generated method stub
    	FileInputFormat.addInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputpathStr));
        if(keyList != null){
	        job.getConfiguration().set(ArgumentsConstants.NORMAL_NOMINAL_KEY_LIST, conf.get(ArgumentsConstants.NORMAL_NOMINAL_KEY_LIST, keyList[0]));
	        job.getConfiguration().set(ArgumentsConstants.CLASS_NOMINAL_KEY_LIST, conf.get(ArgumentsConstants.CLASS_NOMINAL_KEY_LIST, keyList[1]));  
        }
        new String();
		job.getConfiguration().set(ArgumentsConstants.INPUT_NODE_NUM, conf.get(ArgumentsConstants.INPUT_NODE_NUM, String.valueOf(numInputNode)));
        new String();
		job.getConfiguration().set(ArgumentsConstants.OUTPUT_NODE_NUM, conf.get(ArgumentsConstants.OUTPUT_NODE_NUM, String.valueOf(numOutputNode)));
        
        job.getConfiguration().set(ArgumentsConstants.HIDDEN_WEIGHT, conf.get(ArgumentsConstants.HIDDEN_WEIGHT, hWeight));
        job.getConfiguration().set(ArgumentsConstants.OUTPUT_WEIGHT, conf.get(ArgumentsConstants.OUTPUT_WEIGHT, oWeight));
	}

	private void set2StepJob1(Job job, Configuration conf, Path p, String outputPathStr, String minList, String maxList, String idxList) throws IOException {
		// TODO Auto-generated method stub
		FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputPathStr));
//        conf.set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, targetList));
        
      
        
        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.NUMERIC_INDEX, conf.get(ArgumentsConstants.NUMERIC_INDEX));
        job.getConfiguration().set(ArgumentsConstants.MIN_LIST, conf.get(ArgumentsConstants.MIN_LIST, minList));
        job.getConfiguration().set(ArgumentsConstants.MAX_LIST, conf.get(ArgumentsConstants.MAX_LIST, maxList));
	}
	
	private void set2StepJob14Test(Job job, Configuration conf, Path p, String outputPathStr, String minList, String maxList) throws IOException {
		// TODO Auto-generated method stub
		FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.TEST_DATA));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputPathStr));
        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.NUMERIC_INDEX, conf.get(ArgumentsConstants.NUMERIC_INDEX));
        job.getConfiguration().set(ArgumentsConstants.MIN_LIST, conf.get(ArgumentsConstants.MIN_LIST, minList));
        job.getConfiguration().set(ArgumentsConstants.MAX_LIST, conf.get(ArgumentsConstants.MAX_LIST, maxList));
	}

	private void set2StepJob2(Job job, Configuration conf, Path p, String mergePath, String outputPath, String typeIdx) throws IOException
    {
		
		FileInputFormat.addInputPath(job, new Path(mergePath));
		
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+outputPath));
        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.TYPE_LIST, conf.get(ArgumentsConstants.TYPE_LIST, typeIdx));
    }
	
    private String getNominalAttrs(Configuration conf, String inputFile) throws IllegalArgumentException, IOException{
    	FileSystem fs = FileSystem.get(conf);
    	FSDataInputStream fin = fs.open(new Path(inputFile));
    	BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

        String readStr, tokens[];
        StringBuffer retVal = new StringBuffer();
        boolean val[] = null;
        while((readStr=br.readLine())!=null)
        {
//        	System.out.println(readStr);
        	tokens = readStr.split(",");
        	val = getDoubleAttr(tokens);
        	
        	for(int i = 0 ; i < val.length; i++){
        		if(val[i]){
        			retVal.append(0);
        		} else {
        			retVal.append(1);
        		}
        		if( i < val.length -1){
        			retVal.append("\t");
        		}
        	}
        	break;
        }
    	return retVal.toString();
    }
    
    private boolean[] getDoubleAttr(String[] columns){
		int len = columns.length;
		boolean ret[] = new boolean[len];
		
		for(int i = 0; i < len ; i++){
			if(isCanBeDouble(columns[i])){
				ret[i] = true;
			} else{
				ret[i] = false;
			}
		}
		
		return ret;
	}
    
    private boolean isCanBeDouble(String val){
		try {
			Double.parseDouble(val);
			return true;
		} catch(NumberFormatException e){
			return false;
		}
		
	}
    
    
    private double[][] getMinMaxCombine(Configuration conf, String outputFile){
    	 String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
    	 double retVal[][] = null;
    	 ArrayList<String []> list = new ArrayList<String[]>();
    	 int len = 0;
    	 
    	try {
			FileSystem fs = FileSystem.get(conf);
	    	FileStatus[] status = fs.listStatus(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) ));
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
	                    len = tokens.length;
	                    list.add(tokens);

	                }

	                br.close();
	                fin.close();
	            }
	        }
	    	
	    	retVal = new double[list.size()][len];
	    	for(int i = 0; i < list.size() ; i++){
	    		String[] line = list.get(i);
	    		for(int j = 0; j < line.length; j++){
	    			retVal[i][j] = Double.parseDouble(line[j]);
	    		}
	    	}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return retVal;
    }
    
    private String jobFileMerge(Configuration conf, Path p, String inputPath, String outputFile){
	   	 	   	 
	   	try {
				FileSystem fs = FileSystem.get(conf);
				FSDataOutputStream fout = fs.create(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/_numericNomalize/"+ outputFile), true);
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
				FileStatus[] status = fs.listStatus(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+ inputPath));
		    	for (int i=0;i<status.length;i++)
		        {
		            Path fp = status[i].getPath();
	
		            if(fp.getName().indexOf("part-")==0)
		            {
		                FSDataInputStream fin = fs.open(fp);
		                BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
	
		                String readStr;
		                while((readStr=br.readLine())!=null)
		                {
		                	bw.write(readStr+"\r\n");
		                	bw.flush();
		                }
	
		                br.close();
		                fin.close();
		            }
		        }
		    	bw.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//	   	System.out.println(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) +"/merge/"+ outputFile).toString());
	   	return new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/_numericNomalize/"+ outputFile).toString();
   }
    
    private String[] job2Combine(Configuration conf, Path p, String inputPath){
	   	 String retVal[] = new String[2];
	   	 
	   	try {
				FileSystem fs = FileSystem.get(conf);
				FileStatus[] status = fs.listStatus(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+ inputPath));
				
		    	for (int i=0;i<status.length;i++)
		        {
//		    		System.out.println(status[i].toString());
		            Path fp = status[i].getPath();
		            if(fp.getName().indexOf("part-")==0)
		            {
		                FSDataInputStream fin = fs.open(fp);
		                BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
	
		                String readStr;
		                while((readStr=br.readLine())!=null)
		                {
//		                	System.out.println(readStr);
		                	if(readStr.split(",")[0].equals("n")){
//		                		System.out.println(readStr);
		                		retVal[0] = readStr.substring(2);
		                	} else if (readStr.split(",")[0].equals("c")){
		                		retVal[1] = readStr.substring(2);
		                	}
//		                	bw.write(readStr+"\r\n");
		                	
		                	//여기서 키로 해쉬맵 두개를 생성해서 클래스 필드의 노미널 속성값, 일반 필드의 노미널 속성값을 추출한 뒤 스트링형태로 반환해줘야 
		                	
		                }
	
		                br.close();
		                fin.close();
		            }
		        }


			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	   	return retVal;
  }
    
 
    /**
     * row data generation for confusion matrix (org-class, pred-class, frequency)
     * @param conf
     * @return
     * @throws Exception
     */
    private boolean confusionMatrixGen(Configuration conf, Path p, String inputPath, int idx) throws Exception
    {
    	conf.set(ArgumentsConstants.DELIMITER, ",");
        @SuppressWarnings("deprecation")
        
		Job job = new Job(conf);
        conf.set(ArgumentsConstants.INPUT_PATH, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)) + "/"+inputPath);
        
        System.out.println(conf.get(ArgumentsConstants.DELIMITER));
        
        conf.set(ArgumentsConstants.OUTPUT_PATH, conf.get(ArgumentsConstants.OUTPUT_PATH) + "validation_tmp");
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, ",");
        
        new String();
//		job.getConfiguration().set(ArgumentsConstants.CLASS_INDEX, String.valueOf(idx));

        job.setJarByClass(MLPDriver.class);

        job.setMapperClass(ConfusionMatrixMapper.class);
        job.setReducerClass(ConfusionMatrixReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: MLP Final Validation Check is not Completeion");
            return false;
        }

        return true;
    }
    
    /**
     * row data generation for confusion matrix (org-class, pred-class, frequency)
     * @param conf
     * @return
     * @throws Exception
     */
    private boolean rmseGen(Configuration conf, Path p, String inputPath, int idx) throws Exception
    {
        @SuppressWarnings("deprecation")
		Job job = new Job(this.getConf());
        conf.set(ArgumentsConstants.INPUT_PATH, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)) + "/"+inputPath);
        conf.set(ArgumentsConstants.OUTPUT_PATH, conf.get(ArgumentsConstants.OUTPUT_PATH) + "validation_tmp");
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
        conf.set(ArgumentsConstants.DELIMITER, ",");

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, ",");
        new String();
        
//        System.out.println(idx);
        
		job.getConfiguration().set(ArgumentsConstants.CLASS_INDEX, String.valueOf(idx));

        job.setJarByClass(MLPDriver.class);	    			
        
        job.setMapperClass(RMSEMapper.class);
        job.setReducerClass(RMSEReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: C45(Rutine) Final Validation Check is not Completeion");
            return false;
        }

        return true;
    }
    
    //에러 출력, 테스트 모드를 위한 신경망 구조 및 테스트 모듈 
    public double activationFS(double val){
		return 1/(1+Math.exp(-val));
	}
	
    private double hiddenWeight[][];
	private double outputWeight[][];
	private int numHiddenNodes;
	private int numInputNodes;
	private int numOutputNodes;
	
	private String g_wHidden;
	private String g_wOutput;
	
	private void buildNN(){		
		hiddenWeight = new double[numHiddenNodes][numInputNodes+1];
		outputWeight = new double[numOutputNodes][numHiddenNodes+1];
		
		String wList[] = g_wHidden.split("\t");
		int len = wList.length;
		for ( int i = 0; i < len; i++){
			String wNodeList[] = wList[i].split(",");
			int subLen = wNodeList.length;
			for(int j = 0; j < subLen ; j++){
				System.out.println(hiddenWeight[i].length);
				hiddenWeight[i][j] = Double.parseDouble(wNodeList[j]);
			}
		}
		
		wList = g_wOutput.split("\t");
		len = wList.length;
		
		for ( int i = 0; i < len; i++){
			String wNodeList[] = wList[i].split(",");
			int subLen = wNodeList.length;
			for(int j = 0; j < subLen ; j++){
				outputWeight[i][j] = Double.parseDouble(wNodeList[j]);
			}
		}
	}
	private void ComputeOuput(double input[], double[] hidden, double[] output) {
		// TODO Auto-generated method stub
		//결과 출력 계산 함수
		double sum = 0;
		
		for(int i = 0; i < numHiddenNodes; i++){
			sum = 0;
			for(int j = 0; j < numInputNodes; j++){
				sum += input[j] * hiddenWeight[i][j];
			}
			sum += hiddenWeight[i][numInputNodes]; //bias 노드 
			hidden[i] = activationFS(sum);
		}
		for(int i = 0; i < numOutputNodes; i++){
			sum = 0;
			for(int j = 0; j < numHiddenNodes; j++){
				sum += hidden[j] * outputWeight[i][j];
			}
			sum += outputWeight[i][numHiddenNodes]; //bias 노드 
			output[i] = activationFS(sum);
		}
	}

    public static void main(String args[]) throws Exception
    {
        int res = ToolRunner.run(new MLPDriver(), args);
        System.exit(res);
    }
}
