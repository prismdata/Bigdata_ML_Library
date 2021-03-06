/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ankus.mapreduce.algorithms.classification.knn;

import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ConfusionMatrixMapper;
import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ConfusionMatrixReducer;
import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ValidationMain;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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

import java.io.File;
import java.io.FileWriter;
import java.net.URI;

/**
 * kNN Driver
 * @desc
 *
 * @version 0.4
 * @date : 2015.04
 * @author Moonie Song
 */
public class kNNDriver extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(kNNDriver.class);
    private String m_initialK = "10";
    private String m_nominalDistBase = "1";
    long endTime = 0;
   	long lTime  = 0;
   	long startTime = 0 ; 
    
    
    private boolean MRJobforkNN(Configuration conf, String outputBaseStr) throws Exception
    {
        Job job = new Job(conf);
        /*
         * Model is Training input data
         * input is Predict target data
         * if model is null or empty then use input as model
         */
        if(conf.get(ArgumentsConstants.TRAINED_MODEL, "").equals(""))
        {
        	String input_path = conf.get(ArgumentsConstants.INPUT_PATH);
        	conf.set(ArgumentsConstants.TRAINED_MODEL, input_path);
        	FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.TRAINED_MODEL));
        }
        else
        {
        	FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        }
        logger.info(conf.get(ArgumentsConstants.TRAINED_MODEL, ""));
        logger.info(conf.get(ArgumentsConstants.INPUT_PATH, ""));
        Path inputPath = new Path(conf.get(ArgumentsConstants.INPUT_PATH));
        FileSystem fs = FileSystem.get(conf);
        if(fs.isFile(inputPath))
        {
            DistributedCache.addCacheFile(new URI(inputPath.toString()), job.getConfiguration());
        }
        else
        {
            FileStatus[] status = fs.listStatus(inputPath);
            for(int i=0; i<status.length; i++) DistributedCache.addCacheFile(new URI(status[i].getPath().toString()), job.getConfiguration());
        }

        FileOutputFormat.setOutputPath(job, new Path(outputBaseStr + "/classifying_result"));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.NOMINAL_INDEX, conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.CLASS_INDEX, conf.get(ArgumentsConstants.CLASS_INDEX));

        job.getConfiguration().set(ArgumentsConstants.K_CNT, conf.get(ArgumentsConstants.K_CNT, m_initialK));
        job.getConfiguration().set(ArgumentsConstants.NOMINAL_DISTANCE_BASE, conf.get(ArgumentsConstants.NOMINAL_DISTANCE_BASE, m_nominalDistBase));
        job.getConfiguration().set(ArgumentsConstants.DISTANCE_OPTION, conf.get(ArgumentsConstants.DISTANCE_OPTION, Constants.CORR_UCLIDEAN));
        job.getConfiguration().set(ArgumentsConstants.DISTANCE_WEIGHT, conf.get(ArgumentsConstants.DISTANCE_WEIGHT, "false"));

        job.setJarByClass(kNNDriver.class);
        
        job.setMapperClass(kNNDistanceComputeMapper.class);
        job.setCombinerClass(kNNLocalNNExtractCombiner.class);
        job.setReducerClass(kNNGlobalNNExtractReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: kNN MR-Job is not Completeion");
            return false;
        }
        
        return true;
    }
    public int run(String[] args) throws Exception
    {
        Configuration conf = this.getConf();        
        if(!ConfigurationVariable.setFromArguments(args, conf))
        {
            logger.info("> MR Job Setting Failed..");
            return 1;
        }
    	    	
        //return false;
        startTime = System.nanoTime();
        // mk-job for knn
        String outputBaseStr = conf.get(ArgumentsConstants.OUTPUT_PATH);
        FileSystem.get(conf).delete(new Path(outputBaseStr));//FOR LOCAL TEST                   
    	logger.info("Output Path  '" + outputBaseStr + "' will be removed ");
    	
        if(!execMRJobforkNN(conf, outputBaseStr)) return 1;
        logger.info("kNN MR-Job is Finished Successfully");

        endTime = System.nanoTime();
		lTime = endTime - startTime;
		
        
		// validation process
        boolean isValidation = false;
        if(conf.get(ArgumentsConstants.IS_VALIDATION_EXEC, "true").equals("true"))
        {
            conf.set(ArgumentsConstants.INPUT_PATH, outputBaseStr + "/classifying_result");
            conf.set(ArgumentsConstants.OUTPUT_PATH, outputBaseStr + "/validation_tmp");
            if(!confusionMatrixGen(conf)) return 1;

            ValidationMain validate = new ValidationMain();
            validate.validationGeneration(FileSystem.get(conf),
                    conf.get(ArgumentsConstants.OUTPUT_PATH),
                    conf.get(ArgumentsConstants.DELIMITER, "\t"),
                    outputBaseStr + "/validation");

            isValidation = true;
        }
        logger.info("Validation Process is Completion");


        // temp delete process
        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            if(isValidation) FileSystem.get(conf).delete(new Path(outputBaseStr + "/validation_tmp"), true);
            logger.info("> Temporary Files are Deleted..");
        }
        System.out.println("kNN PROCESSING TIME(ms) : " + lTime/1000000.0 + "(ms)");
        
        String processTime = String.format("%f", (lTime/1000000.0)/1000);
        logger.info("kNN PROCESSING TIME  :"+processTime);
        
        return 0;
    }
    
    private void write_processingTime(String log_path, long lngTime)
    {
    	try{
          
            // 파일 객체 생성 kNNDriver.class
            File file = new File(log_path) ;
             
            // true 지정시 파일의 기존 내용에 이어서 작성
            FileWriter fw = new FileWriter(file, true) ;
             
            // 파일안에 문자열 쓰기
            fw.write(String.format("%s", (lTime/1000000.0)/1000));
            fw.flush();
 
            // 객체 닫기
            fw.close(); 
             
             
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    private boolean execMRJobforkNN(Configuration conf, String outputBaseStr) throws Exception
    {
        Job job = new Job(this.getConf());

        if((conf.get(ArgumentsConstants.TRAINED_MODEL)==null)
                || conf.get(ArgumentsConstants.TRAINED_MODEL).equals(conf.get(ArgumentsConstants.INPUT_PATH)))
        {
            FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
            job.getConfiguration().set(Constants.DUPLICATE_KEY_EXCEPTION, "true");
            logger.info("Train file and Test File is Same. Class info of Duplicate ID is not consider for Classification");
        }
        else
        {
            FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.TRAINED_MODEL));
            job.getConfiguration().set(Constants.DUPLICATE_KEY_EXCEPTION, "false");
        }

        Path inputPath = new Path(conf.get(ArgumentsConstants.INPUT_PATH));
        FileSystem fs = FileSystem.get(conf);
        if(fs.isFile(inputPath))
        {
            DistributedCache.addCacheFile(new URI(inputPath.toString()), job.getConfiguration());
        }
        else
        {
            FileStatus[] status = fs.listStatus(inputPath);
            for(int i=0; i<status.length; i++) DistributedCache.addCacheFile(new URI(status[i].getPath().toString()), job.getConfiguration());
        }

        FileOutputFormat.setOutputPath(job, new Path(outputBaseStr + "/classifying_result"));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
//        job.getConfiguration().set(ArgumentsConstants.KEY_INDEX, conf.get(ArgumentsConstants.KEY_INDEX));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.NOMINAL_INDEX, conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.CLASS_INDEX, conf.get(ArgumentsConstants.CLASS_INDEX));

        job.getConfiguration().set(ArgumentsConstants.K_CNT, conf.get(ArgumentsConstants.K_CNT, m_initialK));
        job.getConfiguration().set(ArgumentsConstants.NOMINAL_DISTANCE_BASE, conf.get(ArgumentsConstants.NOMINAL_DISTANCE_BASE, m_nominalDistBase));
        job.getConfiguration().set(ArgumentsConstants.DISTANCE_OPTION, conf.get(ArgumentsConstants.DISTANCE_OPTION, Constants.CORR_UCLIDEAN));
        job.getConfiguration().set(ArgumentsConstants.DISTANCE_WEIGHT, conf.get(ArgumentsConstants.DISTANCE_WEIGHT, "false"));

        job.setJarByClass(kNNDriver.class);

        job.setMapperClass(kNNDistanceComputeMapper.class);
        job.setCombinerClass(kNNLocalNNExtractCombiner.class);
        job.setReducerClass(kNNGlobalNNExtractReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: kNN MR-Job is not Completeion");
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
    private boolean confusionMatrixGen(Configuration conf) throws Exception
    {
        Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.CLASS_INDEX, conf.get(ArgumentsConstants.CLASS_INDEX));

        job.setJarByClass(kNNDriver.class);

        job.setMapperClass(ConfusionMatrixMapper.class);
        job.setReducerClass(ConfusionMatrixReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: kNN Final Validation Check is not Completeion");
            return false;
        }

        return true;
    }


    public static void main(String args[]) throws Exception
    {
        int res = ToolRunner.run(new kNNDriver(), args);
        System.exit(res);
    }
}
