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

/**
 * Constants
 * @desc
 *      Collected constants of general utility
 * @version 0.0.1
 * @date : 2013.07.15
 * @author Suhyun Jeon
 * @author Moonie Song
 */
public class Constants {
    public static final String DRIVER_BOOLEAN_DATA_CORRELATION = "BooleanDataCorrelation";
   
    // for common
    public static String DATATYPE_BOOLEAN = "boolean";
    public static String DATATYPE_NUMERIC = "numeric";
    public static String DATATYPE_NOMINAL = "nominal";
    public static String COMMON_MAP_OUTPUT_CNT = "mapOutputRecordCnt";

    public static String ATTR_CLASS = "class";

    // for correlation/similarity classes
    public static final String CORR_HAMMING = "hamming";
    public static final String CORR_DICE = "dice";
    public static final String CORR_JACCARD = "jaccard";
    public static final String CORR_TANIMOTO = "tanimoto";
    public static final String CORR_MANHATTAN = "manhattan";
    public static final String CORR_UCLIDEAN = "uclidean";
    public static final String CORR_COSINE = "cosine";
    public static final String CORR_PEARSON = "pearson";
    public static final String CORR_EDIT = "edit";
    public static final String CORR_MATCHING = "matching";

    public static final String KEY_INDEX = "keyIndex";
    public static final String TARGET_INDEX = "indexList";
    public static final String DELIMITER = "delimiter";
    // Separate of multi data to one column
    public static final String SUB_DELIMITER = "subDelimiter";
    public static final String ALGORITHM_OPTION = "algorithmOption";
    public static final String COMPUTE_INDEX = "computeIndex";
    public static final String THRESHOLD = "threshold";
    public static final String REMOVE_INDEX = "removeIndex";
    public static final String BASED_TYPE = "basedType";
    public static final String COMMON_COUNT = "commonCount";

    /**
     * Remove mode for midterm process
     */
    public static final String REMOVE_ON = "on";
    public static final String REMOVE_OFF = "off";
    public static final String MIDTERM_PROCESS_OUTPUT_DIR = "midterm.process.output.dir";
    public static final String MIDTERM_PROCESS_OUTPUT_REMOVE_MODE = "midterm.process.output.remove.mode";

    // etc.
    public static final String UTF8 = "UTF-8";
    public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";





}
