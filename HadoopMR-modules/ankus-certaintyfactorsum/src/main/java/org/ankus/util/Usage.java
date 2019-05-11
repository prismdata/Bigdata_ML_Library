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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 사용법 출력을 제어하는 클래스.
 * @version 0.0.1
 * @date : 2013.08.10
 * @modify : 2013.12.10
 * @author Suhyun Jeon
 */
public class Usage {

    // SLF4J Logging
    private static Logger logger = LoggerFactory.getLogger(Usage.class);
    /**
     * 사용법을 출력하는 함수.
     * @author Suhyun Jeon
     * @date 2013.12.10
     * @param String algorithm : 알고리즘 명칭.
     */
    public static void printUsage(String algorithm){

        String ankusVersionJarName = "ankus-core2-certaintyfactorsum-1.1.0.jar";
        String delimiterSeparateValues = "< {tab | comma | colon} >";
        String subDelimiterSeparateValues = "< {tab | comma | colon} >";

        // Each algorithms description
        String description = null;
        // Each algorithms parameter
        StringBuffer parameters = new StringBuffer();

       if(algorithm.equals(Constants.DRIVER_CERTAINTYFACTOR_SUM)){
            description = "Certainty Factor based Summation based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index_list>]\n");
            parameters.append("      	  <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.EXCEPTION_INDEX + " <index_list>] default value: -1\n");
            parameters.append("           [" + ArgumentsConstants.CERTAINTY_FACTOR_MAX + " <max_value>] default value: 1\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
            parameters.append("           [" + ArgumentsConstants.MR_JOB_STEP + " <1|2>]  default value: 1\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <true|false>] default value: true\n");
        }
       
        logger.info("=========================================================================================================");
        logger.info(description);
        logger.info("---------------------------------");
        logger.info(parameters.toString());
        logger.info("=========================================================================================================");

    }
}
