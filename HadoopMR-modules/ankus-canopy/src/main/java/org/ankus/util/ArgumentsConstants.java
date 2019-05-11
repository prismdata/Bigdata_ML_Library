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
 * ArgumentsConstants
 * @desc
 *      Collected constants of parameter by user
 * @version 0.0.1
 * @date : 2013.08.23
 * @author Suhyun Jeon
 * @author Moonie Song
 */
public class ArgumentsConstants {

    // common
    public static final String INPUT_PATH = "-input";
    public static final String OUTPUT_PATH = "-output";
    public static final String DELIMITER = "-delimiter";
    public static final String TARGET_INDEX = "-indexList";
    
    public static final String NOMINAL_INDEX = "-nominalIndexList";
    public static final String EXCEPTION_INDEX = "-exceptionIndexList";
    public static final String TEMP_DELETE = "-tempDelete";
    public static final String HELP = "-help";          // current not used, for CLI
    
    // for canopy
    public static final String CANOPY_T1 = "-t1";
    public static final String CANOPY_T2 = "-t2";
    
    // for classification and clustering
    public static final String DISTANCE_OPTION = "-distanceOption";
    public static final String FINAL_RESULT_GENERATION = "-finalResultGen";
    public static final String TRAINED_MODEL = "-modelPath";    
}
