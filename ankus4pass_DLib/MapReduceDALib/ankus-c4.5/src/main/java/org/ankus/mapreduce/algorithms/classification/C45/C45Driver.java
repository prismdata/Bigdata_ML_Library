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

package org.ankus.mapreduce.algorithms.classification.C45;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.ankus.mapreduce.algorithms.classification.C45.C45RuleMgr;
import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ConfusionMatrixMapper;
import org.ankus.mapreduce.algorithms.classification.confusionMatrix.ConfusionMatrixReducer;
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
 * C45
 * @desc
 *
 * @version 0.1
 * @date : 2016.04.05
 * @author SHINHONGJOONG
 */

/*
 C45 \
-input /32.C45/weather.csv \
-output /32.C45_OUTPUT \
-indexList 0,1,2,3 \
-classIndex 4 \
-minLeafData 10 \
-finalResultGen true \
-delimiter , 
 */
public class C45Driver extends Configured implements Tool {

	
    private Logger logger = LoggerFactory.getLogger(C45Driver.class);
    private String m_minData = "5";
    //private String m_purity = "0.75";
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
       
        String output_path = conf.get(ArgumentsConstants.OUTPUT_PATH, null);
        FileSystem.get(conf).delete(new Path(output_path));//FOR LOCAL TEST                   
        logger.info("Output Path  '" + output_path + "' will be removed ");
        startTime = System.nanoTime();
        /*
        if, train - model generation
            temp delete check

        test (model or final-result-gen)
            classify
            if class index exist > validation
            else no validation

            if(final-result-gen) train classify result remove
         */
        String numeric_idx = conf.get(ArgumentsConstants.TARGET_INDEX, "-1");
        String norminal_idx = conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1");
        if(numeric_idx == norminal_idx)
        {
        	 logger.info("Error: Please check input index list");
             return 1;
        }
        boolean isOnlyTest = false;
        boolean isTrained = false;
        boolean isTrainResultGen = false;
        boolean isValidation = false;
        if(conf.get(ArgumentsConstants.TRAINED_MODEL, null) != null) isOnlyTest = true;

        int iterCnt = 0;
        String outputBase = conf.get(ArgumentsConstants.OUTPUT_PATH, null);
        String ruleFilePath = outputBase + "/C45_rule.txt";
        String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

        // training process
        if(!isOnlyTest)
        {
        	if(conf.get(ArgumentsConstants.CLASS_INDEX, "-1").equals("-1"))
            {
                logger.info("> Class Index is must defined for Training in C45(Ankus).");
                return 1;
            }

            conf.set(ArgumentsConstants.MIN_LEAF_DATA, conf.get(ArgumentsConstants.MIN_LEAF_DATA, m_minData));
            conf.set(ArgumentsConstants.DELIMITER, delimiter);

            logger.info("Purity for Pruning: " + conf.get(ArgumentsConstants.PURITY));
            logger.info("Minimum Lef Node Count for Pruning: " + conf.get(ArgumentsConstants.MIN_LEAF_DATA));
            logger.info("> C45 Classification Iterations (Training) are Started..");
            logger.info("> : Information Gain Computation and Rule Update for Every Tree Node");

            String nodeStr;
            C45RuleMgr C45RuleMgr = new C45RuleMgr();
            while((nodeStr = C45RuleMgr.loadNonLeafNode(conf))!=null)
            {
                String tokens[] = nodeStr.split(conf.get(ArgumentsConstants.DELIMITER));
                
                //root or generate new rule as Non leaf 
                conf.set(Constants.C45_RULE_CONDITION, tokens[0]); 
                conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/entropy_" + iterCnt);

                @SuppressWarnings("deprecation")
				Job job = new Job(this.getConf());
                
                FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
                FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)), true);
                FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));

                job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
                job.getConfiguration().set(ArgumentsConstants.NOMINAL_INDEX, conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1"));
                
//                job.getConfiguration().set(ArgumentsConstants.NUMERIC_INDEX, conf.get(ArgumentsConstants.NUMERIC_INDEX, "-1"));
                job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
                
                job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
                job.getConfiguration().set(ArgumentsConstants.CLASS_INDEX, conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
                job.getConfiguration().set(ArgumentsConstants.MIN_LEAF_DATA, conf.get(ArgumentsConstants.MIN_LEAF_DATA, "1"));
                job.getConfiguration().set(ArgumentsConstants.PURITY, conf.get(ArgumentsConstants.PURITY, "1"));
                job.getConfiguration().set(Constants.C45_RULE_CONDITION, conf.get(Constants.C45_RULE_CONDITION, "root"));
                
                job.setJobName(conf.get(ArgumentsConstants.INPUT_PATH));
                job.setJarByClass(C45Driver.class);
                job.setMapperClass(C45AttributeSplitMapper.class);
                job.setReducerClass(C45ComputeEntropyReducer.class);//Get All GainRatio

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);
                
                if(!job.waitForCompletion(true))
                {
                    logger.info("Error: 1-MR for C45(Rutine) is not Completeion");
                    return 1;
                }
              
                String oldRulePath = conf.get(ArgumentsConstants.RULE_PATH);
                conf.set(ArgumentsConstants.RULE_PATH, ruleFilePath + "_" + iterCnt);
            
                if(C45RuleMgr.updateRule(conf, oldRulePath, nodeStr) == -1)
                {
                	return 1;
                }
                iterCnt++;
            }
            FileSystem.get(conf).rename(new Path(ruleFilePath + "_" + (iterCnt-1)), new Path(ruleFilePath));
            logger.info("> C45 Classification Iterations are Finished..");
    		
            if(conf.get(ArgumentsConstants.FINAL_RESULT_GENERATION, "false").equals("true"))
                isTrainResultGen = true;
        }
        endTime = System.nanoTime();
		lTime = endTime - startTime;
        // test process | final result gen in training
        FileSystem fs = FileSystem.get(conf);
        if(isOnlyTest || isTrainResultGen)
        {
            if(isOnlyTest) ruleFilePath = conf.get(ArgumentsConstants.TRAINED_MODEL);
            
            if(FileSystem.get(conf).exists(new Path(ruleFilePath)))
            {
	            conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/classifying_result");
	            conf.set(ArgumentsConstants.RULE_PATH, ruleFilePath);
	            if(!finalClassifying(conf)) 
	        	{
	            	return 1;
	        	}
	            
	            if(!conf.get(ArgumentsConstants.CLASS_INDEX, "-1").equals("-1"))
	            {
	                // class index exist
	                conf.set(ArgumentsConstants.INPUT_PATH, conf.get(ArgumentsConstants.OUTPUT_PATH));
	                conf.set(ArgumentsConstants.OUTPUT_PATH, outputBase + "/validation_tmp");
	                if(!confusionMatrixGen(conf)) return 1;
	                ValidationMain validate = new ValidationMain();
	                
	                validate.validationGeneration(fs,
	                        conf.get(ArgumentsConstants.OUTPUT_PATH),
	                        conf.get(ArgumentsConstants.DELIMITER, "\t"),
	                        outputBase + "/validation.txt");
	
	                isValidation = true;
	            }
//	            mFileIntegration(fs, outputBase + "/classifying_result", "part-m");
	            logger.info("> C45(Ankus) Classification Using Trained Model is Finished..");
            }
        }
       
        // temp delete process
        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            //if(isTrained)
            if(true)
            {
                for(int i=0; i<iterCnt-1; i++)
                {
                    FileSystem.get(conf).delete(new Path(outputBase + "/entropy_" + i), true);
                    FileSystem.get(conf).delete(new Path(ruleFilePath + "_" + i), true);
                }
                FileSystem.get(conf).delete(new Path(outputBase + "/entropy_" + (iterCnt-1)), true);
            }

            if(isValidation) FileSystem.get(conf).delete(new Path(outputBase + "/validation_tmp"), true);
            logger.info("> Temporary Files are Deleted..");
        }
        
        System.out.println("C4.5 PROCESSING TIME(ms) : " + lTime/1000000.0 + "(ms)");
		
        return 0;
    }
    
    private void mFileIntegration(FileSystem fs , String inputPath, String filePrefix)
    {
    	try
    	{
	    	FileStatus[] status = fs.listStatus(new Path(inputPath));
	    	FSDataOutputStream fout = fs.create(new Path(inputPath + "/total_result.csv"), true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
            
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
	            	bw.write(readStr);
	            	bw.write("\r\n");
	            }
	            br.close();
	            fin.close();
	            
	            fs.delete(fp, true);
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

        job.setJarByClass(C45Driver.class);

        job.setMapperClass(ConfusionMatrixMapper.class);
        job.setReducerClass(ConfusionMatrixReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: C45(Rutine) Final Validation Check is not Completeion");
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
        job.getConfiguration().set(ArgumentsConstants.RULE_PATH, conf.get(ArgumentsConstants.RULE_PATH));

        job.setJarByClass(C45Driver.class);

        job.setMapperClass(C45FinalClassifyingMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        if(!job.waitForCompletion(true))
        {
            logger.info("Error: C45(Rutine) Final Mapper is not Completeion");
            return false;
        }

        return true;
    }
    
    public int C45_main(String args[]) throws Exception
    {
    	 int res = ToolRunner.run(new C45Driver(), args);
         return res;
    }
    public static void main(String args[]) throws Exception
    {
        int res = ToolRunner.run(new C45Driver(), args);
        System.exit(res);
    }
}
