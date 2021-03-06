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
package org.ankus.mapreduce.algorithms.classification.naivebayes;

import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ConfusionMatrixMapper;
import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ConfusionMatrixReducer;
import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ValidationMain;
//import org.ankus.mapreduce.algorithms.classification.id3.ID3AttributeSplitMapper;
//import org.ankus.mapreduce.algorithms.classification.id3.ID3ComputeEntropyReducer;
//import org.ankus.mapreduce.algorithms.classification.id3.ID3FinalClassifyingMapper;
//import org.ankus.mapreduce.algorithms.classification.rulestructure.RuleMgr;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;

/**
 * NaiveBayesDriver
 * @desc
 *
 * @version 0.1
 * @date : 2015.05.12
 * @author Moonie Song, HongJoong.Shin
 */
public class NaiveBayesDriver extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(NaiveBayesDriver.class);
    long endTime = 0;
   	long lTime  = 0;
   	long startTime = 0 ; 
    public int run(String[] args) throws Exception
    {
        Configuration conf = this.getConf();
        if(!ConfigurationVariable.setFromArguments(args, conf))
        {
            logger.info("> MR Job Setting Failed..");
            return 1;
        }

        /*

        if, train - model generation
            temp delete check

        test (model or final-result-gen)
            classify
            if class index exist > validation
            else no validation

            if(final-result-gen) train classify result remove
         */

        boolean isOnlyTest = false;
        if(conf.get(ArgumentsConstants.TRAINED_MODEL, null) != null) isOnlyTest = true;

        boolean isTrained = false;
        boolean isTrainResultGen = false;
        boolean isValidation = false;

        String outputBase = conf.get(ArgumentsConstants.OUTPUT_PATH, null);
        String ruleFilePath = outputBase + "/bayes_rules";
        String mrOutputPath = ruleFilePath + "_mr";
        String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        
        //Load Class Labels
        Job jobGetLabels = new Job(this.getConf());
        FileSystem label_fs = FileSystem.get(conf);
        label_fs.delete(new Path(outputBase + "_classLabels"));
        FileInputFormat.addInputPaths(jobGetLabels, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(jobGetLabels, new Path(outputBase + "_classLabels"));
        
        jobGetLabels.setJarByClass(NaiveBayesDriver.class);
        jobGetLabels.setMapperClass(Map_LabelLoader.class);
        jobGetLabels.setReducerClass(Reducer_LabelWriter.class);
        
        jobGetLabels.setOutputKeyClass(Text.class);
        jobGetLabels.setOutputValueClass(NullWritable.class);

        if(!jobGetLabels.waitForCompletion(true))
        {
            logger.info("Error: MR for NaiveBayes(Rutine) is not Completeion");
            return -1;
        }
        String labelpath = outputBase + "_classLabels";
        FileStatus[] status = label_fs.listStatus(new Path(labelpath));
        String ClassList = "";
        for (int i=0;i<status.length;i++)
        {
        	Path fp = status[i].getPath();
        	String fname = fp.getName();
            if(fname.indexOf("part-")==0)
            {
                FSDataInputStream fin = label_fs.open(status[i].getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
                String readStr, tokens[];
                while((readStr=br.readLine())!=null)
                {
                	System.out.println(readStr);
                	ClassList += readStr + ",";
                }

                br.close();
                fin.close();
            }
        }
        ClassList = ClassList.substring(0, ClassList.length()-1);
        conf.set(ArgumentsConstants.CLASS_LIST, ClassList);
        // training process
        if(!isOnlyTest)
        {
            if(conf.get(ArgumentsConstants.CLASS_INDEX, "-1").equals("-1"))
            {
                logger.info("> Class Index must be defined for Training in NaiveBayes(Ankus).");
                return 1;
            }
            conf.set(ArgumentsConstants.DELIMITER, delimiter);
            startTime = System.nanoTime();
            long totalDataCnt = computePriorProbability(conf, mrOutputPath);
            if(totalDataCnt <= 0)
            {
                logger.error("Error: MR Job for NaiveBayes is Failed: Mapper Count is Zero");
                return 1;
            }
            logger.info("> MR Job for NaiveBayes is Finished..");

            mergeRules(conf, mrOutputPath, ruleFilePath, totalDataCnt);
            logger.info("> Rule Merging and Final Result Generation for Naive Bayes is Finished..");

            isTrained = true;
            if(conf.get(ArgumentsConstants.FINAL_RESULT_GENERATION, "false").equals("true"))
                isTrainResultGen = true;
        }
        endTime = System.nanoTime();
		lTime = endTime - startTime;
        // test process | final result gen in training
        if(isOnlyTest || isTrainResultGen)
        {
            if(isOnlyTest) ruleFilePath = conf.get(ArgumentsConstants.TRAINED_MODEL);

            conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/classifying_result");
            conf.set(ArgumentsConstants.RULE_PATH, ruleFilePath);
            if(!finalClassifying(conf)) return 1;

            if(!conf.get(ArgumentsConstants.CLASS_INDEX, "-1").equals("-1"))
            {
                // class index exist
                conf.set(ArgumentsConstants.INPUT_PATH, conf.get(ArgumentsConstants.OUTPUT_PATH));
                conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/validation_tmp");
                if(!confusionMatrixGen(conf)) return 1;

                ValidationMain validate = new ValidationMain();
                FileSystem fs = FileSystem.get(conf);
                validate.validationGeneration(fs,
                        conf.get(ArgumentsConstants.OUTPUT_PATH),
                        conf.get(ArgumentsConstants.DELIMITER, "\t"),
                        outputBase + "/validation");

                isValidation = true;
            }
            logger.info("> NaiveBayes(Ankus) Classification Using Trained Model is Finished..");
        }

        // temp delete process
        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            if(isTrained) FileSystem.get(conf).delete(new Path(mrOutputPath), true);
            if(isValidation) FileSystem.get(conf).delete(new Path(outputBase + "/validation_tmp"), true);

            logger.info("> Temporary Files are Deleted..");
        }
        System.out.println("NaiveBayes PROCESSING TIME(ms) : " + lTime/1000000.0 + "(ms)");
        
        return 0;
    }
   
    private long computePriorProbability(Configuration conf, String outputPath) throws Exception
    {
        Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.CLASS_INDEX, conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));

        job.setJarByClass(NaiveBayesDriver.class);
        job.setMapperClass(NBSplitMapper.class);
        job.setCombinerClass(NBStatSumCombiner.class);
        job.setReducerClass(NBStatSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: MR for NaiveBayes(Rutine) is not Completeion");
            return -1;
        }

        // long totalMapperCnt = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
        long totalMapperCnt = job.getCounters().findCounter("NAIVEBAYES", "MAPCOUNT").getValue();

        return totalMapperCnt;
    }

    private boolean mergeRules(Configuration conf, String inputPath, String outputFileName, long totalDataCnt) throws Exception
    {
        // reducer file merge and
        HashMap<String, Long> classInfoMap = new HashMap<String, Long>();

        String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream fout = fs.create(new Path(outputFileName), true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
        bw.write("# AttrIndex(or Class)" + delimiter +
                "Type" + delimiter +
                "Value(Category or Avg/StdDev)" + delimiter +
                "ValueCount" + delimiter +
                "ClassType" + delimiter +
                "ClassCount" + "\n");

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
                    bw.write(readStr + "\n");

                    tokens = readStr.split(delimiter);
                    if(!classInfoMap.containsKey(tokens[tokens.length-2]))
                    {
                        classInfoMap.put(tokens[tokens.length-2], Long.parseLong(tokens[tokens.length-1]));
                    }
                }

                br.close();
                fin.close();
            }
        }

        // HashMap and Total Data Count Writing
        Iterator<String> classIter = classInfoMap.keySet().iterator();
        while(classIter.hasNext())
        {
            String classStr = classIter.next();
            String writeStr = Constants.ATTR_CLASS + delimiter
                                + classStr + delimiter
                                + classInfoMap.get(classStr) + delimiter
                                + totalDataCnt;
            bw.write(writeStr + "\n");
        }

        bw.close();
        fout.close();
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

        job.setJarByClass(NaiveBayesDriver.class);

        job.setMapperClass(ConfusionMatrixMapper.class);
        job.setReducerClass(ConfusionMatrixReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: NaiveBayes Final Validation Check is not Completeion");
            return false;
        }

        return true;
    }

    /**
     * classification result generation for train file (add class info to train data file)
     * @param conf
     * @return
     * @throws Exception
     */
    private boolean finalClassifying(Configuration conf) throws Exception
    {
        Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));

        // TODO: Distributed Cache File: Rule File
        FileSystem fs = FileSystem.get(conf);
        DistributedCache.addCacheFile(new URI(conf.get(ArgumentsConstants.RULE_PATH)), job.getConfiguration());

        job.setJarByClass(NaiveBayesDriver.class);

        job.setMapperClass(NBClassifyingMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: NaiveBayes Final Mapper(for classification) is not Completeion");
            return false;
        }

        return true;
    }

    public static void main(String args[]) throws Exception
    {
        int res = ToolRunner.run(new NaiveBayesDriver(), args);
        System.exit(res);
    }
}
