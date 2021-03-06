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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * ConfigurationVariable
 * @desc
 * @version 0.0.1
 * @date : 2013.08.23
 * @author Moonie Song
 */
public class ConfigurationVariable {

    // SLF4J Logging
    private static Logger logger = LoggerFactory.getLogger(ConfigurationVariable.class);

    private static boolean isDefinedArgumentName(String str)
    {
        if(str.equals(ArgumentsConstants.INPUT_PATH)
				|| str.equals(ArgumentsConstants.OUTPUT_PATH)
				|| str.equals(ArgumentsConstants.LOCAL_LOG_PATH) //수행 시간 기록용 경로
                || str.equals(ArgumentsConstants.DELIMITER)
                || str.equals(ArgumentsConstants.TARGET_INDEX)
                || str.equals(ArgumentsConstants.NOMINAL_INDEX)
                || str.equals(ArgumentsConstants.NUMERIC_INDEX)
                || str.equals(ArgumentsConstants.EXCEPTION_INDEX)
                || str.equals(ArgumentsConstants.MR_JOB_STEP)
                || str.equals(ArgumentsConstants.TEMP_DELETE)
                || str.equals(ArgumentsConstants.HELP)
                || str.equals(ArgumentsConstants.CLASS_INDEX)

                || str.equals(ArgumentsConstants.K_CNT)
                || str.equals(ArgumentsConstants.DISTANCE_WEIGHT)
                || str.equals(ArgumentsConstants.IS_VALIDATION_EXEC)
                || str.equals(ArgumentsConstants.NOMINAL_DISTANCE_BASE)

                || str.equals(ArgumentsConstants.DISTANCE_OPTION)
                || str.equals(ArgumentsConstants.TRAINED_MODEL)
		) return true;
		
        return false;
		
	}
	
	@SuppressWarnings("deprecation")
	public static boolean setFromArguments(String[] args, Configuration conf) throws IOException 
	{
		String argName = "";
		String argValue = "";
		String yamlPath = "host_config.yaml";
			
		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		File config = new File(yamlPath);
		HadoopHostConfig hostConfig = mapper.readValue(config, HadoopHostConfig.class);
		
		conf.set("fs.default.name",  hostConfig.getFs_default_name());
		
		for (int i=0; i<args.length; ++i) 
        {
			argName = args[i];
			
			if(isDefinedArgumentName(argName))
			{
                argValue = args[++i];
                if (argName.equals(ArgumentsConstants.OUTPUT_PATH) && FileSystem.get(conf).exists(new Path(argValue)))
                {
                	boolean keepOutput = conf.getBoolean(ArgumentsConstants.KEEP_OUTPUT_PATH, false);
                	if(keepOutput == false)
                	{
	                	FileSystem.get(conf).delete(new Path(argValue));//FOR LOCAL TEST                   
	                	logger.info("Output Path  '" + argValue + "' will be removed ");
                	}
                    //return false;
                }
                else if(argName.equals(ArgumentsConstants.DELIMITER))
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
