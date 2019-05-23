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

package org.ankus.mapreduce.algorithms.clustering.canopy;


import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.ankus.util.Usage;
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;

/**
 *
 * @desc
 *
 * @version 0.4.0
 * @date : 2015.08.
 * @author Moonie
 */
public class CanopyDriver extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(CanopyDriver.class);
    long endTime = 0;
   	long lTime  = 0;
   	long startTime = 0 ; 
    @Override
	public int run(String[] args) throws Exception
	{
        logger.info("Canopy Clustering MR-Job is Started..");

		Configuration conf = this.getConf();
		//conf.set("fs.default.name",  "hdfs://localhost:9000");
		if(!ConfigurationVariable.setFromArguments(args, conf))
		{
			logger.error("MR Job Setting Failed..");
            Usage.printUsage(Constants.DRIVER_CANOPY_CLUSTERING);
            logger.info("Error: MR Job Setting Failed..: Configuration Failed");
            return 1;
		}
		startTime = System.nanoTime();
        // train - find canopy centers
		logger.info("Processing 1/3 - find canopy centers");
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/CanopyResult"), true);
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/canopy"), true);
        String canopyOutput = "/canopy";
        if(!getCanopyCenters(conf, canopyOutput))
        {
            logger.info(">> MR-Job for Clustering (get canopy centers) is Failed..");
            return 1;
        }

        // train result gen - canopy center notify
        // if train result gen 이면, 데이터에 center 여부를 표기 한다.
        String trainOutput = "/CanopyResult";
        logger.info("Processing 2/3 - canopy center notify");
        if(conf.get(ArgumentsConstants.FINAL_RESULT_GENERATION, "true").equals("true"))
        {
            if(!markCanopyCenters(conf, trainOutput, canopyOutput))
            {
                logger.info(">> MR-Job for Clustering (making canopy centers) is Failed..");
                return 1;
            }
        }
        
        // temp deletetion
        logger.info("Processing 3/3 - temp deletetion");
        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            logger.info(">> There is no temporary files..");
        }
        logger.info(">> MR-Job is successfully finished..");
        endTime = System.nanoTime();
		lTime = endTime - startTime;
		
		System.out.println("Canopy Clustering Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
        return 0;
	}

    boolean markCanopyCenters(Configuration conf, String outputStr, String canopyOutput) throws Exception
    {
        Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputStr));

        job.getConfiguration().set("CANOPY_CENTER", conf.get(ArgumentsConstants.OUTPUT_PATH) + canopyOutput);
        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));

        job.setJarByClass(CanopyDriver.class);

        job.setMapperClass(CanopyFinalMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        if(!job.waitForCompletion(true))
        {
            logger.error("Error: MR for Canopy Marking on Train File is not Completeion");
            return false;
        }
        FileSystem fs = FileSystem.get(conf);
		String inputpath = conf.get(ArgumentsConstants.OUTPUT_PATH);
		mFileIntegration(fs, 
								inputpath,
								"part-m", 
								".csv");
        return true;

    }
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
	        System.out.println("ClusterDistribution");
	        System.out.println(cluster_count.toString());
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.toString());
    	}
    }
    boolean getCanopyCenters(Configuration conf, String outputStr) throws Exception
    {
        Job job = new Job(this.getConf());

        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputStr));

        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.NOMINAL_INDEX, conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));

        job.getConfiguration().set(ArgumentsConstants.CANOPY_T1, conf.get(ArgumentsConstants.CANOPY_T1, "1"));
        job.getConfiguration().set(ArgumentsConstants.CANOPY_T2, conf.get(ArgumentsConstants.CANOPY_T2, "1"));
        job.getConfiguration().set(ArgumentsConstants.DISTANCE_OPTION, conf.get(ArgumentsConstants.DISTANCE_OPTION, Constants.CORR_UCLIDEAN));

        job.setJarByClass(CanopyDriver.class);

        job.setMapperClass(CanopyMapper.class);
        job.setCombinerClass(CanopyLocalCenterCombiner.class);
        job.setReducerClass(CanopyGlobalCenterReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.error("Error: MR for Canopy CLustering is not Completeion");
            return false;
        }

        return true;
    }



	public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new CanopyDriver(), args);
        System.exit(res);
	}



}