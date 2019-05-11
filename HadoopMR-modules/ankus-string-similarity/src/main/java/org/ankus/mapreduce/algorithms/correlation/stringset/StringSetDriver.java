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

package org.ankus.mapreduce.algorithms.correlation.stringset;

import org.ankus.io.*;
import org.ankus.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;

/**
 * StringSetDriver
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Hamming distance 2. Edit distance
 * @version 0.0.1
 * @date : 2013.07.11
 * @author Suhyun Jeon
*/
public class StringSetDriver extends Configured implements Tool {

    private String input = null;
    private String output = null;
    private String keyIndex = null;
    private String computeIndex = null;
    private String algorithmOption = null;
    private String delimiter = null;

    private FileSystem fileSystem = null;

    // SLF4J Logging
    private Logger logger = LoggerFactory.getLogger(StringSetDriver.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new StringSetDriver(), args);
        System.exit(res);
    }

    @SuppressWarnings("unused")
	public int run(String[] args) throws Exception {

        if(args.length < 1){
            Usage.printUsage(Constants.DRIVER_STRING_DATA_CORRELATION);
            return -1;
        }
        long endTime = 0;
       	long lTime  = 0;
       	long startTime = 0 ; 
       	
       	startTime = System.nanoTime();
       	
        initArguments(args);

        // Get key (midterm.process.output.remove.mode) from config.properties
        Properties configProperties = CommonMethods.getConfigProperties();
        String removeModeMidtermProcess = configProperties.get(Constants.MIDTERM_PROCESS_OUTPUT_REMOVE_MODE).toString();
        boolean removeMode = false;
        if(removeModeMidtermProcess.equals(Constants.REMOVE_ON)){
            removeMode = true;
        }

        removeMode = true;

        // Get prepare output path for in the middle of job processing
        String prepareDirectory = CommonMethods.createDirectoryForHDFS(output);
        String prepareOutput = prepareDirectory + "/" + algorithmOption + "/";
        
        Configuration conf = this.getConf();
//        conf.set("fs.default.name",  "hdfs://localhost:9000");
        fileSystem = FileSystem.get(conf);
        
        URI fileSystemUri = fileSystem.getUri();
        Path prepareOutputPath = new Path(fileSystemUri + "/" + prepareOutput);

        logger.info("==========================================================================================");
        logger.info("Prepare output directory is [" + prepareOutputPath.toString() + "]");
        logger.info("==========================================================================================");

        Job job1 = new Job(this.getConf());
        job1 = HadoopJopPrepare.prepareJob(job1, new Path(input), prepareOutputPath, StringSetDriver.class,
                StringSetMapper.class, Text.class, TextTwoWritableComparable.class,
                StringSetReducer.class, TextTwoWritableComparable.class, TextFourWritableComparable.class);
        
        FileSystem.get(conf).delete(new Path(output), true);
        FileSystem.get(conf).delete(prepareOutputPath, true);
        
		job1.getConfiguration().set(Constants.DELIMITER, delimiter);
		job1.getConfiguration().set(Constants.KEY_INDEX, keyIndex);
		job1.getConfiguration().set(Constants.COMPUTE_INDEX, computeIndex);
		boolean step1 = job1.waitForCompletion(true);
        if(!(step1)) return -1;


        Job job2 = new Job(this.getConf());
        job2 = HadoopJopPrepare.prepareJob(job2, prepareOutputPath, new Path(output), StringSetDriver.class,
                CalculationStringSetMapper.class, TextTwoWritableComparable.class, TextFourWritableComparable.class,
                CalculationStringSetReducer.class, TextTwoWritableComparable.class, DoubleWritable.class);

        job2.getConfiguration().set(Constants.DELIMITER, delimiter);
        job2.getConfiguration().set(Constants.ALGORITHM_OPTION, algorithmOption);
        
        boolean step2 = job2.waitForCompletion(true);
        if(!(step2)) return -1;

        // Remove all midterm process output files.
        if(removeMode){
            boolean delete = fileSystem.delete(prepareOutputPath, true);
            if(delete){
                logger.info("Delete midterm process output files.");
            }
        }
        endTime = System.nanoTime();
		lTime = endTime - startTime;
		
		System.out.println("String Similarity  Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
		System.out.format("String Similarity  Finished Time : %f Seconds\n", (lTime/1000000.0)/1000);
        return 0;
    }

    private void initArguments(String[] args) {
        try{
            for (int i = 0; i < args.length; ++i) {
                if (ArgumentsConstants.INPUT_PATH.equals(args[i])) {
                    input = args[++i];
                } else if (ArgumentsConstants.OUTPUT_PATH.equals(args[i])) {
                    output = args[++i];
                } else if (ArgumentsConstants.KEY_INDEX.equals(args[i])) {
                    keyIndex = args[++i];
                } else if (ArgumentsConstants.ALGORITHM_OPTION.equals(args[i])) {
                    algorithmOption = args[++i];
                } else if (ArgumentsConstants.DELIMITER.equals(args[i])) {
                	 int di =++i;
                	 if(args[di].equals("t")||args[di].equals("\\t")||args[di].equals("'\t'")||args[di].equals("\"\t\"") ||args[di].equals(""))
                     {
                		 delimiter = "\t";
                     }
                	 else
                	 {
                		 delimiter = ",";
                	 }
                } else if (ArgumentsConstants.COMPUTE_INDEX.equals(args[i])) {
                    computeIndex = args[++i];
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}