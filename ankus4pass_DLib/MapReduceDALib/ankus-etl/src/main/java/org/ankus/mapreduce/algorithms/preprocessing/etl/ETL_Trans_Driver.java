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

package org.ankus.mapreduce.algorithms.preprocessing.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.ankus.util.Usage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * ETL알고리즘을 수행하는 클래스 
 * @author HongJoong.Shin
 * @date   2016.12.06
 */
public class ETL_Trans_Driver extends Configured implements Tool {
	private Logger logger = LoggerFactory.getLogger(ETL_Trans_Driver.class);
	
	/**
	 * 각 ETL Method에 대한 기능을 열거형으로 선언함. 
	 * @auth HongJoong.Shin
     * @date 2016.12.06
	*/
	private enum ETL_T_Method {
		//Replace, ColumnExtractor, ColumnValueExclude, ColumnValueFilter, Sort;
		Replace, ColumnExtractor, FilterInclude, FilterExclude, Sort, NumericNorm, Transform, xlsImport;
	}
	/**
     * main()함수로 ToolRunner를 사용하여 ETL 기능을 호출한다.
     * @author  HongJoong.Shin
     * @parameter String[] args : 유사도 분석 알고리즘 수행 인자.
     * @author HongJoong.Shin
     * @date   2016.12.06
     * @return 없음.
     */
	public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new ETL_Trans_Driver(), args);
        System.exit(res);
	}
	 /**
     * ToolRunner에서 호출되는 실제 알고리즘 시작 함수.
     * @auth HongJoong.Shin
     * @date 2016.12.06
     * @parameter String[] args : ETL  분석 알고리즘 수행 인자.
     * @return int
     */
	@Override
	public int run(String[] args) throws Exception
	{
        logger.info("Fiter MR-Job is Started..");
		
		Configuration conf = this.getConf();
		if(!ConfigurationVariable.setFromArguments(args, conf))
		{
            Usage.printUsage(Constants.DRIVER_ETL_FILTER);
            logger.info("Error: MR Job Setting Failed..: Configuration Failed");
            return 1;
		}	
		
		String ETL_T_method =  conf.get(ArgumentsConstants.ETL_T_METHOD, null);
		if(ETL_T_method == null)
		{
			logger.info("Error: Needs filter method");
			return 1;
		}
		
		ETL_T_Method method = ETL_T_Method.valueOf(ETL_T_method); // surround with try/catch
		switch(method) 
		{
		    case FilterInclude:
		    	String filter_rule_path = conf.get(ArgumentsConstants.ETL_RULE_PATH, null);
				if(filter_rule_path == null)
				{
					String filter_rule = conf.get(ArgumentsConstants.ETL_RULE, null);
					if(filter_rule == null)
					{
						logger.info("Error: Needs Filter rule as exclude when Rule path doesn't exist for ColumnValueExclude method");
						return 1;
					}
				}
		    	break;
		}		
		String filter_output = conf.get(ArgumentsConstants.OUTPUT_PATH, null);
		if(filter_output == null)
		{
			logger.info("Error: Needs output path list");
			return 1;
		}
	
        logger.info("FilterDriver Step of MR-Job is Started..");
        Job job = new Job(this.getConf());       
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
		job.getConfiguration().set(ArgumentsConstants.DELIMITER, conf.get(ArgumentsConstants.DELIMITER, "\t"));
		job.getConfiguration().set(ArgumentsConstants.TARGET_INDEX, conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
       
        int numReduceTasks = 1;
        String partitionLocation = filter_output  + "/partition";
        logger.info(method.toString());
        job.setJobName(method.toString());
        switch(method) 
		{
		    case ColumnExtractor:
		    case FilterInclude:
		    case Replace:
		    case FilterExclude:
		    case NumericNorm:
		    	job.setJarByClass(ETL_Trans_Driver.class);
		        job.setMapperClass(ETL_FilterMapper.class);
		        job.setReducerClass(ETL_FilterReducer.class);
		        job.setMapOutputKeyClass(NullWritable.class);
		        job.setMapOutputValueClass(Text.class);
//		        job.setNumReduceTasks(1);
		        if(!job.waitForCompletion(true))
		    	{
		        	logger.error("Error: MR for FilterDriver is not Completion");
		            logger.info("MR-Job is Failed..");
		        	return 1;
		        }		 
		    	break;
		    	
		    case Sort:
		    	job.setJarByClass(ETL_Trans_Driver.class);
		        job.setMapperClass(ETL_SortMapper.class);
		        job.setReducerClass(ETL_SortReducer.class);		        
		        
		        job.setInputFormatClass(TextInputFormat.class);
		        job.setOutputFormatClass(TextOutputFormat.class);
		 
		        job.setOutputKeyClass(Text.class);
		        job.setOutputValueClass(Text.class);
		        
		        //input parameter에 따라 오름/내림 정렬 클래스 변경.
		        String sort_method = conf.get(ArgumentsConstants.ETL_NUMERIC_SORT_METHOD, "asc");
		        sort_method = sort_method.toLowerCase();
		        if(sort_method.equals("asc") == true)
		        {
		        	job.setSortComparatorClass(SortKeyComparator_Ascending.class);
		        }
		        else if(sort_method.equals("desc") == true)
		        {
		        	job.setSortComparatorClass(SortKeyComparator_descending.class);
		        }
		        else
		        {
		        	logger.error("Unknown sort method. Please use asc or desc");
		        	return 1;		        	
		        }
		        FileInputFormat.setInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));		        
		        String output_path = conf.get(ArgumentsConstants.OUTPUT_PATH);
		        FileOutputFormat.setOutputPath(job, new Path(output_path));		       
		        try 
		        {
		            job.waitForCompletion(true);
		        }
		        catch (InterruptedException ex) 
		        {
		            logger.error(ex.toString());
		        }
		        catch (ClassNotFoundException ex) 
		        {
		            logger.error(ex.toString());
		        }
		    	break;
		}
       // sort_valiation(conf,conf.get(ArgumentsConstants.OUTPUT_PATH)+"/part-r-00000",  conf.get(ArgumentsConstants.ETL_NUMERIC_SORT_METHOD, "asc"), true);

        logger.info("FilterDriver Step of MR-Job is Successfully Finished...");
        return 0;
        
	}
		 
}
