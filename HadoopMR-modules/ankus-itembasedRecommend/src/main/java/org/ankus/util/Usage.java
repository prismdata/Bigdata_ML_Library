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
 * 알고리즘 수행 인자 오류 발생시 수행 방법을 출력하는 클래스.
 * 
 * @version 0.0.1
 * @date : 2013.08.10
 * @modify : 2013.12.10
 * @author Suhyun Jeon
 */
public class Usage {

    // SLF4J Logging
    private static Logger logger = LoggerFactory.getLogger(Usage.class);

    /**
     * 알고리즘 수행 방법을 출력함.
     * @author Suhyun Jeon
     * @param String algorithm : 알고리즘 명칭
     */
    public static void printUsage(String algorithm){

        String ankusVersionJarName = "ankus-core2-itembasedRecommend-1.1.0.jar";
        String delimiterSeparateValues = "< {tab | comma | colon} >";

        // Each algorithms description
        String description = null;
        // Each algorithms parameter
        StringBuffer parameters = new StringBuffer();

        if(algorithm.equals(Constants.DRIVER_RECOMMENDATION)){
            description = "all based recommendation system based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");
            parameters.append("           [" + ArgumentsConstants.UID_INDEX + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.IID_INDEX + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.RATING_INDEX + " <index>]\n");
            
            parameters.append("           [" + ArgumentsConstants.SIMILARITY_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.SIMILARITY_DELIMITER + " " + delimiterSeparateValues + "]\n");
            parameters.append("           [" + ArgumentsConstants.SIMILARITY_THRESHOLD + " <0~5>]\n");
            
            parameters.append("           [" + ArgumentsConstants.TARGET_UID + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_IID_LIST + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.RECOMMENDATION_CNT + " <0~>]\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <{true|false}>]\n");
        }
        
        logger.info("=========================================================================================================");
        logger.info(description);
        logger.info("---------------------------------");
        logger.info(parameters.toString());
        logger.info("=========================================================================================================");

    }
}