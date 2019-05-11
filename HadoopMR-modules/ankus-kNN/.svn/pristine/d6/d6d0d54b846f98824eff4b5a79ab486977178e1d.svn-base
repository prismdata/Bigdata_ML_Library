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
 * Usage
 * @desc
 *      Display format of commands
 * @version 0.0.1
 * @date : 2013.08.10
 * @modify : 2013.12.10
 * @author Suhyun Jeon
 */
public class Usage {

    // SLF4J Logging
    private static Logger logger = LoggerFactory.getLogger(Usage.class);

    public static void printUsage(String algorithm){

        String ankusVersionJarName = "ankus-kNN-0.1.jar";
        String delimiterSeparateValues = "< {tab | comma | colon} >";
        String subDelimiterSeparateValues = "< {tab | comma | colon} >";

        // Each algorithms description
        String description = null;
        // Each algorithms parameter
        StringBuffer parameters = new StringBuffer();
        
        if(algorithm.equals(Constants.DRIVER_kNN_CLASSIFICATION)){
            description = "kNN driver based on map/reduce program that computes the data of the boolean set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index_list>]\n");
            parameters.append("           [" + ArgumentsConstants.CLASS_INDEX + " <class_index>])\n");
            parameters.append("           [" + ArgumentsConstants.K_CNT + "<Numbers of neighborhoods>])\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.TRAINED_MODEL + "<model path>])\n");
            parameters.append("           [" + ArgumentsConstants.NOMINAL_DISTANCE_BASE + "<distance value for nominal default value: 1>])\n");
            parameters.append("           [" + ArgumentsConstants.DISTANCE_OPTION + "<manhattan, uclidean>])\n");
            parameters.append("           [" + ArgumentsConstants.DISTANCE_WEIGHT + "<Weight Option default value: false>])\n");
            parameters.append("           [" + ArgumentsConstants.IS_VALIDATION_EXEC + "<Use Validation  default value: true>])\n");
            parameters.append("           [" + ArgumentsConstants.EXCEPTION_INDEX + " <index_list>] default value: -1\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
        }
        logger.info("=========================================================================================================");
        logger.info(description);
        logger.info("---------------------------------");
        logger.info(parameters.toString());
        logger.info("=========================================================================================================");

    }
}
