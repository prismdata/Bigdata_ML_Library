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

        String ankusVersionJarName = "ankus-core-0.1.jar";
        String delimiterSeparateValues = "< {tab | comma | colon} >";
        String subDelimiterSeparateValues = "< {tab | comma | colon} >";

        // Each algorithms description
        String description = null;
        // Each algorithms parameter
        StringBuffer parameters = new StringBuffer();

        // Correlation modules
        if(algorithm.equals(Constants.DRIVER_BOOLEAN_DATA_CORRELATION)){
            description = "BooleanSet driver based on map/reduce program that computes the data of the boolean set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.KEY_INDEX + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.ALGORITHM_OPTION + " <{ jaccard | dice | hamming }>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");

        }else if(algorithm.equals(Constants.DRIVER_NUMERIC_DATA_CORRELATION)){
            description = "NumericSet driver based on map/reduce program that computes the data of the numeric set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + "<path>]\n");
            parameters.append("           [" + ArgumentsConstants.KEY_INDEX + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.ALGORITHM_OPTION + " <{ cosine | pearson | manhattan | uclidean }>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");

        }else if(algorithm.equals(Constants.DRIVER_STRING_DATA_CORRELATION)){
            description = "StringSet driver based on map/reduce program that computes the data of the string set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + "<path>]\n");
            parameters.append("           [" + ArgumentsConstants.KEY_INDEX + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.ALGORITHM_OPTION + " <{ edit | hamming }>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");

        // CF(Collaborative Filtering) based similarity modules
        }else if(algorithm.equals(Constants.DRIVER_CF_BASED_SIMILARITY)){
            description = "Collaborative filtering based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + "<path>]\n");
            parameters.append("           [" + ArgumentsConstants.BASED_TYPE + " <string>]\n");
            parameters.append("           [" + ArgumentsConstants.ALGORITHM_OPTION + " <{ cosine | pearson }>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");

       // Content based similarity modules
        }else if(algorithm.equals(Constants.DRIVER_CONTENT_BASED_SIMILARITY)){
            description = "Content based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + "<path>]\n");
            parameters.append("           [" + ArgumentsConstants.KEY_INDEX + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.ALGORITHM_OPTION + " <{ cosine | pearson }>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");
            parameters.append("           [" + ArgumentsConstants.SUB_DELIMITER + " " + subDelimiterSeparateValues + "]\n");

        // Content based similarity modules
        }else if(algorithm.equals(Constants.DRIVER_RECOMMENDATION)){
            description = "all based recommendation system based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.SIMILARITY_DATA_INPUT + " <index>]\n");
            parameters.append("           [" + ArgumentsConstants.BASED_TYPE + " <user | item>]\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "]\n");

        // Statistics modules
        }else if(algorithm.equals(Constants.DRIVER_NUMERIC_STATS)){
            description = "Numeric Statistics Computation based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index_list>]\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.EXCEPTION_INDEX + " <index_list>] default value: -1\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
            parameters.append("           [" + ArgumentsConstants.MR_JOB_STEP + " <1|2>]  default value: 1\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <true|false>] default value: true\n");

        }else if(algorithm.equals(Constants.DRIVER_NOMINAL_STATS)){
            description = "Nominal Statistics(frequency/ratio) Computation based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index>]\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <true|false>] default value: true\n");

        }else if(algorithm.equals(Constants.DRIVER_CERTAINTYFACTOR_SUM)){
            description = "Certainty Factor based Summation based on map/reduce program that computes the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index_list>]\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.EXCEPTION_INDEX + " <index_list>] default value: -1\n");
            parameters.append("           [" + ArgumentsConstants.CERTAINTY_FACTOR_MAX + " <max_value>] default value: 1\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
            parameters.append("           [" + ArgumentsConstants.MR_JOB_STEP + " <1|2>]  default value: 1\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <true|false>] default value: true\n");

        // Preprocessing modules
        }else if(algorithm.equals(Constants.DRIVER_NORMALIZE)){
            description = "Numeric Data Normalization based on map/reduce program that converts the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index_list>]\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.EXCEPTION_INDEX + " <index_list>] default value: -1\n");
            parameters.append("           [" + ArgumentsConstants.REMAIN_FIELDS + " <true|false>] default value: true\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
            parameters.append("           [" + ArgumentsConstants.MR_JOB_STEP + " <1|2>]  default value: 1(for numeric stats)\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <true|false>] default value: true\n");

        // Clustering modules
        }else if(algorithm.equals(Constants.DRIVER_KMEANS_CLUSTERING)){
            description = "K-Means Clustering based on map/reduce program that uses the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index_list>]\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.NOMINAL_INDEX + " <index_list>] default value: -1\n");
            parameters.append("           [" + ArgumentsConstants.EXCEPTION_INDEX + " <index_list>] default value: -1\n");
            parameters.append("           [" + ArgumentsConstants.NORMALIZE + " <true|false>] default value: true\n");
            parameters.append("           [" + ArgumentsConstants.MAX_ITERATION + " <count>] default value: 1(do not recommend)\n");
            parameters.append("           [" + ArgumentsConstants.CLUSTER_COUNT + " <count>] default value: 1(do not recommend)\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <true|false>] default value: true\n");

        // Clustering modules
        }else if(algorithm.equals(Constants.DRIVER_EM_CLUSTERING)){
            description = "EM Clustering based on map/reduce program that uses the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index_list>]\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.NOMINAL_INDEX + " <index_list>] default value: -1\n");
            parameters.append("           [" + ArgumentsConstants.EXCEPTION_INDEX + " <index_list>] default value: -1\n");
            parameters.append("           [" + ArgumentsConstants.MAX_ITERATION + " <count>] default value: 1(do not recommend)\n");
            parameters.append("           [" + ArgumentsConstants.CLUSTER_COUNT + " <count>] default value: 1(do not recommend)\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");
            parameters.append("           [" + ArgumentsConstants.TEMP_DELETE + " <true|false>] default value: true\n");

        // Classification modules
        }else if(algorithm.equals(Constants.DRIVER_ID3_CLASSIFICATION)){
            description = "EM Classification based on map/reduce program that uses the data of the data set in the input files.";
            parameters.append(" hadoop jar " + ankusVersionJarName + " " + algorithm + " ");
            parameters.append("\n");
            parameters.append("           [" + ArgumentsConstants.INPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.OUTPUT_PATH + " <path>]\n");
            parameters.append("           [" + ArgumentsConstants.CLASS_INDEX + " <class_index>])\n");
            parameters.append("      <optional parameter>:\n");
            parameters.append("           [" + ArgumentsConstants.TARGET_INDEX + " <index_list>]\n");
            parameters.append("           [" + ArgumentsConstants.EXCEPTION_INDEX + " <index_list>] default value: -1\n");
            parameters.append("           [" + ArgumentsConstants.MIN_LEAF_DATA + " <count>] default value: 1\n");
            parameters.append("           [" + ArgumentsConstants.PURITY + " <purity>] default value: 0.75\n");
            parameters.append("           [" + ArgumentsConstants.FINAL_RESULT_GENERATION + " <true|false>] default value: false\n");
            parameters.append("           [" + ArgumentsConstants.DELIMITER + " " + delimiterSeparateValues + "] default value: tab\n");

         
        }//2015 03 27 whitepoo@onycom.com
        else if(algorithm.equals(Constants.DRIVER_PFPGROWTH_ASSOCIATION)){
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
