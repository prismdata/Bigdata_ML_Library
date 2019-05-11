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
 * @version 0.0.1
 * @date : 2016.xx.xx
 * @author HongJoong.Shin
 */
public class Usage {
    private static Logger logger = LoggerFactory.getLogger(Usage.class);
    /**
     * 알고리즘의 사용법을 출력합니다.
     * @author HongJoong.Shin
     * @param String algorithm : 알고리즘 명칭
     * @return
     */
    public static void printUsage(String algorithm){

        String ankusVersionJarName = "ankus-Document-sim-1.1.0.jar";
        String delimiterSeparateValues = "< {tab | comma | colon} >";

        // Each algorithms description
        String description = null;
        // Each algorithms parameter
        StringBuffer parameters = new StringBuffer();

        if(algorithm.equals(Constants.DRIVE_DOCSIMILITY)){
            description = "Document Similarity driver based on map/reduce program that computes similarity of each document in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");
        }
      
        logger.info("=========================================================================================================");
        logger.info(description);
        logger.info("---------------------------------");
        logger.info(parameters.toString());
        logger.info("=========================================================================================================");

    }
}
