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
 * 알고리즘 수행 파라미터 검증과 파싱
 * @version 0.0.1
 * @date : 2016.12.6
 * @author HongJoong.Shin
 */
public class ConfigurationVariable {

    // SLF4J Logging
    private static Logger logger = LoggerFactory.getLogger(ConfigurationVariable.class);
    /**
     * 등록되어 있는 알고리즘 인자인지 검사.
     * @author HongJoong.Shin
     * @parameter String str : 알고리즘 인자 
     * @date : 2016.12.6
     * @version 0.0.1
     * @return 필수 인자가 모두 설정되었을 경우 true, 정의되지 않은 인자가 기입된 경우 false리턴.
     */
    private static boolean isDefinedArgumentName(String str)
    {
        if(str.equals(ArgumentsConstants.INPUT_PATH)
			|| str.equals(ArgumentsConstants.OUTPUT_PATH)
            || str.equals(ArgumentsConstants.DELIMITER)
            || str.equals(ArgumentsConstants.TEMP_DELETE)
            || str.equals(ArgumentsConstants.AR_MINSUPP)
            || str.equals(ArgumentsConstants.AR_MAX_RULE_LENGTH)
            || str.equals(ArgumentsConstants.AR_METRIC_TYPE)
            || str.equals(ArgumentsConstants.AR_METRIC_VALUE)
            || str.equals(ArgumentsConstants.AR_RULE_COUNT)
            || str.equals(ArgumentsConstants.AR_TARGET_ITEM)            
            
		) return true;
        return false;
	}
    /**
	 * 알고리즘 수행 인자 파싱
	 * @author HongJoong.Shin
	 * @parameter String[] args : 알고리즘 인자 
	 * @parameter Configuration conf : 하둡 환경 설정 변수
	 * @date : 2016.12.6
	 * @version 0.0.1
	 * @return 정상적으로 인자가 설정된 경우 true, 정의되지 않은 인자가 설정될 경우 false를 리턴.
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
