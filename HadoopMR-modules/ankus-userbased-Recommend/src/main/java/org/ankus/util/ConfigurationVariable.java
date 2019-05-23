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
 * 알고리즘 수행 변수 검사 클래
 * @desc
 * @version 0.0.1
 * @date : 2013.08.23
 * @author Moonie Song
 */
public class ConfigurationVariable {

    // SLF4J Logging
    private static Logger logger = LoggerFactory.getLogger(ConfigurationVariable.class);
/**
 * 미등록 알고리즘 수행 인자 검사.
 * @param String str : 수행 인자
 * @return boolean
 */
    private static boolean isDefinedArgumentName(String str)
    {
        if(str.equals(Constants.INPUT_PATH)
				|| str.equals(Constants.OUTPUT_PATH)
                || str.equals(Constants.DELIMITER)
                || str.equals(Constants.SUB_DELIMITER)
                || str.equals(Constants.UID_INDEX)
                || str.equals(Constants.IID_INDEX)
                || str.equals(Constants.RATING_INDEX)
                || str.equals(Constants.BASED_TYPE)
                || str.equals(Constants.SIMILARITY_DELIMITER)
                || str.equals(Constants.SIMILARITY_PATH)
                || str.equals(Constants.SIMILARITY_THRESHOLD)
                || str.equals(Constants.RECOMMENDATION_CNT)
                || str.equals(Constants.TARGET_UID)
                || str.equals(Constants.TARGET_IID_LIST)

                || str.equals(Constants.USER_INDEX)
                || str.equals(Constants.ITEM_INDEX)
                || str.equals(Constants.THRESHOLD)
                || str.equals(Constants.SIMILARITY_DATA_INPUT)
                || str.equals(Constants.RECOMMENDED_DATA_INPUT)
                
				) return true;
		
        return false;
		
	}
	
    /**
     * 모듈 실행 인자를 분석.
     * @param String[] args 실행 파라이터, Configuration conf 하둡 환경 변수.
     * @throws Exception
     */
	@SuppressWarnings("deprecation")
	public static boolean setFromArguments(String[] args, Configuration conf) throws IOException 
	{
		String argName = "";
		String argValue = "";
		
//		conf.set("fs.default.name",  "hdfs://localhost:9000");
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
                if(argName.equals(Constants.DELIMITER))
                {
                    if(argValue.equals("t")||argValue.equals("\\t")||argValue.equals("'\t'")||argValue.equals("\"\t\"") ||argValue.equals(""))
                    {
                        argValue = "\t";
                    }
                }
                else if(argName.equals(Constants.SUB_DELIMITER))
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