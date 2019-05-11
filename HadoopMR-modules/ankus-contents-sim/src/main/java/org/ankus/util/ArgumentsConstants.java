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
 * 알고리즘 인자 상수 정의
 * @version 0.0.1
 * @date : 2013.08.23
 * @author Suhyun Jeon
 * @author Moonie Song
 */
public class ArgumentsConstants {

    // common
    public static final String INPUT_PATH = "-input";
    public static final String OUTPUT_PATH = "-output";
    public static final String LOCAL_LOG_PATH = "-logpath";
    public static final String KEEP_OUTPUT_PATH = "-keep_output";
    public static final String DELIMITER = "-delimiter";
    public static final String SUB_DELIMITER = "-subDelimiter";
    public static final String TARGET_INDEX = "-indexList";    

    public static final String EXCEPTION_INDEX = "-exceptionIndexList";
    public static final String HELP = "-help";          // current not used, for CLI
  
    // for correlation and similarity
    public static final String KEY_INDEX = "-keyIndex";                     // contents based sim
    public static final String ALGORITHM_OPTION = "-algorithmOption";       // cf-sim, contents based sim

    public static final String TARGET_ID = "-targetID";
    public static final String CORRVALLIMIT = "-corrValLimit";
    public static final String SUMMATION_OPTION = "-sumOption";

    public static final String TEMP_DELETE = "-tempDelete";
}
