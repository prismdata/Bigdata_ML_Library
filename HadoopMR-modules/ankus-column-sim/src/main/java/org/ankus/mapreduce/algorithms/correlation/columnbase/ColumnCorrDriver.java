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

package org.ankus.mapreduce.algorithms.correlation.columnbase;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
 * Created with IntelliJ IDEA.
 * User: Wonmoon
 * Date: 14. 10. 21
 * Time: 오후 5:47
 * To change this template use File | Settings | File Templates.
 */
public class ColumnCorrDriver extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(ColumnCorrDriver.class);

    public static void main(String args[]) throws Exception
    {
        int res = ToolRunner.run(new ColumnCorrDriver(), args);
        System.exit(res);
    }

    private String mAlgoOpt_Base = Constants.CORR_UCLIDEAN;

    @Override
    public int run(String[] args) throws Exception
    {
        logger.info("Correlation/Similarity Computation between Column(Attribute) Data is Started..");

        Configuration conf = this.getConf();
//        conf.set("fs.default.name",  "hdfs://localhost:9000");
        if(!ConfigurationVariable.setFromArguments(args, conf))
        {
            logger.error("MR Job Setting Failed..");
            return 1;
        }

        // argument > index-list, algorithm-option

        long endTime = 0;
       	long lTime  = 0;
       	long startTime = 0 ; 
       	
       	startTime = System.nanoTime();
       	
        Job job = new Job(this.getConf());
        
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
        
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
        job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
        job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.EXCEPTION_INDEX, conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        job.getConfiguration().set(ArgumentsConstants.ALGORITHM_OPTION, conf.get(ArgumentsConstants.ALGORITHM_OPTION, mAlgoOpt_Base));

        job.setJarByClass(ColumnCorrDriver.class);

        job.setMapperClass(ColumnCorrMapper.class);
        job.setReducerClass(ColumnCorrReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true))
        {
            logger.error("Error: MR for Column based Correlation is not Completeion");
            logger.info("MR-Job is Failed..");
            return 1;
        }
        endTime = System.nanoTime();
		lTime = endTime - startTime;
		
		System.out.println("Correlation/Similarity Computation Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
        
        logger.info("MR-Job is successfully finished..");
        return 0;
    }
}