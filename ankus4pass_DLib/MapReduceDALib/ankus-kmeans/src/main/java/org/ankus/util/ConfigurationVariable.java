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
 * 알고리즘 수행 인자의 시스템 적용과 검증.
 * @version 0.0.1
 * @date : 2013.08.23
 * @author Moonie Song
 */
public class ConfigurationVariable {

    // SLF4J Logging
    private static Logger logger = LoggerFactory.getLogger(ConfigurationVariable.class);
    
    /**
    * 정의된 인자 확인. 
    * @param  String str : 사용자가 인자.
    * @return boolean 등록된 인자 true, 등록되지 않은 인자 false
    */
    private static boolean isDefinedArgumentName(String str)
    {
        if(str.equals(ArgumentsConstants.INPUT_PATH)
			|| str.equals(ArgumentsConstants.OUTPUT_PATH)
            || str.equals(ArgumentsConstants.DELIMITER)
            || str.equals(ArgumentsConstants.TARGET_INDEX)
            || str.equals(ArgumentsConstants.NOMINAL_INDEX)
            || str.equals(ArgumentsConstants.EXCEPTION_INDEX)
            || str.equals(ArgumentsConstants.MR_JOB_STEP)
            || str.equals(ArgumentsConstants.TEMP_DELETE)
            || str.equals(ArgumentsConstants.HELP)
            || str.equals(ArgumentsConstants.CLUSTER_COUNT)    
            || str.equals(ArgumentsConstants.CLUSTER_TRAINING_CONVERGE)   
            || str.equals(ArgumentsConstants.MAX_ITERATION)   
            || str.equals(ArgumentsConstants.FINAL_RESULT_GENERATION)   
            || str.equals(ArgumentsConstants.TRAINED_MODEL)
            || str.equals(ArgumentsConstants.NORMALIZE)   
            || str.equals(ArgumentsConstants.REMAIN_FIELDS)   
  
		) return true;
		
        return false;
		
	}
    /**
	* 환경 변수 설정.
	* @param String[] args : 사용자 인자.
	* @param Configuration conf : 하둡 환경 변수.
	* @return 정상 인자: true, 등록되지 않은 인자: false
	 */
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
                if(argName.equals(ArgumentsConstants.DELIMITER))
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
