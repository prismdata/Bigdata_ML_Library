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
 * @date : 2016.12.6
 * @version 0.0.1
 * @author HongJoong.Shin
 */
public class Usage {

    // SLF4J Logging
    private static Logger logger = LoggerFactory.getLogger(Usage.class);
    /**
     * 알고리즘의 사용법을 출력합니다.
     * @author HongJoong.Shin
     * @date : 2016.12.6
     * @version 0.0.1
     * @parameter String algorithm : 알고리즘 명칭
     * @return 없음.
     */
    public static void printUsage(String algorithm){

        String ankusVersionJarName = "ankus-core2-FPGrowth-1.1.0.jar";
        String delimiterSeparateValues = "< {tab | comma | colon} >";
        String subDelimiterSeparateValues = "< {tab | comma | colon} >";

        // Each algorithms description
        String description = null;
        // Each algorithms parameter
        StringBuffer parameters = new StringBuffer();

        //2015 03 27 whitepoo@onycom.com
        if(algorithm.equals(Constants.DRIVER_PFPGROWTH_ASSOCIATION)){
            description = "PFPGrowth based on map/reduce program that uses the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.AR_MINSUPP + " default value:2]\n");
            parameters.append("           [" + ArgumentsConstants.AR_MAX_RULE_LENGTH + " required]\n");
            parameters.append("           [" + ArgumentsConstants.AR_METRIC_TYPE + " required]\n");
            parameters.append("           [" + ArgumentsConstants.AR_METRIC_VALUE + " required]\n");
            parameters.append("           [" + ArgumentsConstants.AR_RULE_COUNT + " required]\n");
            parameters.append("           [" + ArgumentsConstants.AR_TARGET_ITEM + " required]\n");
        }

        logger.info("=========================================================================================================");
        logger.info(description);
        logger.info("---------------------------------");
        logger.info(parameters.toString());
        logger.info("=========================================================================================================");

    }
}
