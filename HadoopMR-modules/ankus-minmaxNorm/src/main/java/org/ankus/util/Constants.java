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
* MapReduce 수행 관련 상수정의 

 * @version 0.0.1
 * @date : 2013.07.15
 * @author Suhyun Jeon
 * @author Moonie Song
 */
public class Constants {

	public static final String DRIVER_NORMALIZE = "Normalization";
	public static final String DRIVER_NUMERIC_STATS = "NumericStatistics";
	// for statistics
    public static String STATS_MINMAX_VALUE = "minMaxValue";
    public static String STATS_NUMERIC_QUARTILE_COUNTER = "NUMERIC_STAT_BLOCK_DATA_CNT";
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
    public static final String UTF8 = "UTF-8";
}
