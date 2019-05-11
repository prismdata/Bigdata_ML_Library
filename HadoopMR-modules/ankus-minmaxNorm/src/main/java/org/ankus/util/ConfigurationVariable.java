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

package org.ankus.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 알고리즘 환경 변수 설정
 * @desc
 * @version 0.0.1
 * @date : 2013.08.23
 * @author Moonie Song
 */
public class ConfigurationVariable {

    // SLF4J Logging
    private static Logger logger = LoggerFactory.getLogger(ConfigurationVariable.class);
    /**
     * 등록되어 있는 알고리즘 인자인지 검사.
     * @auth HongJoong.Shin
     * @parameter String str : 알고리즘 인자 
     * @return boolean
     */
    private static boolean isDefinedArgumentName(String str)
    {
        if(str.equals(ArgumentsConstants.INPUT_PATH)
				|| str.equals(ArgumentsConstants.OUTPUT_PATH)
				|| str.equals(ArgumentsConstants.LOCAL_LOG_PATH) //수행 시간 기록용 경로
                || str.equals(ArgumentsConstants.DELIMITER)
                || str.equals(ArgumentsConstants.SUB_DELIMITER)
                || str.equals(ArgumentsConstants.TARGET_INDEX)
                || str.equals(ArgumentsConstants.NOMINAL_INDEX)
                || str.equals(ArgumentsConstants.NUMERIC_INDEX)
                || str.equals(ArgumentsConstants.EXCEPTION_INDEX)
                || str.equals(ArgumentsConstants.MR_JOB_STEP)
                || str.equals(ArgumentsConstants.TEMP_DELETE)
                || str.equals(ArgumentsConstants.HELP)

                || str.equals(ArgumentsConstants.REMAIN_FIELDS)
                || str.equals(ArgumentsConstants.DISCRETIZATION_COUNT)      
              
                || str.equals(ArgumentsConstants.FILTER_TARGET_INDEX)
                
                ) return true;
		
        return false;
		
	}
    /**
	 * 알고리즘 수행 인자 파싱
	 * @auth HongJoong.Shin
	 * @parameter String[] args : 알고리즘 인자 
	 * @parameter Configuration conf : 하둡 환경 설정 변수
	 * @return boolean
	 */
	@SuppressWarnings("deprecation")
	public static boolean setFromArguments(String[] args, Configuration conf) throws IOException 
	{
		String argName = "";
		String argValue = "";
		
//		conf.set("fs.default.name",  "hdfs://localhost:9000");
//		
//		conf.set("hadoop.tmp.dir",  "/Users/hadoop/Documents/hadoop-2.6.0/tmp");		
//		conf.set("mapreduce.cluster.temp.dir",  "/Users/hadoop/Documents/hadoop-2.6.0/tmp/mapred/temp");		
//		conf.set("mapreduce.cluster.local.dir",  "/Users/hadoop/Documents/hadoop-2.6.0/tmp/mapred/local");		
//		conf.set("mapreduce.jobtracker.system.dir",  "/Users/hadoop/Documents/hadoop-2.6.0/tmp/mapred/system");		
//		conf.set("mapreduce.jobtracker.staging.root.dir",  "/Users/hadoop/Documents/hadoop-2.6.0/tmp/mapred/staging");
		
		for (int i=0; i<args.length; ++i) 
        {
			argName = args[i];
			
			if(isDefinedArgumentName(argName))
			{
                argValue = args[++i];
                if (argName.equals(ArgumentsConstants.OUTPUT_PATH) && FileSystem.get(conf).exists(new Path(argValue)))
                {
                	
//                	logger.info("Output Path  '" + argValue + "' is exist ");
//                    return false;
                }
                else if(argName.equals(ArgumentsConstants.DELIMITER))
                {
                    if(argValue.equals("t")||argValue.equals("\\t")||argValue.equals("'\t'")||argValue.equals("\"\t\"") ||argValue.equals(""))
                    {
                        argValue = "\t";
                    }
                }
                else if(argName.equals(ArgumentsConstants.SUB_DELIMITER))
                {
                    if(argValue.equals("t")||argValue.equals("\\t")||argValue.equals("'\t'")||argValue.equals("\"\t\"") ||argValue.equals(""))
                    {
                        argValue = "\t";
                    }
                }
                conf.set(argName, argValue);
			}
			else 
			{
                logger.error("Argument Error: Unknowned Argument '" + argName + "'");
				return false;
			}
        }
        
        return true;
	}

}
