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
 * 알고리즘 수행에 사용되는 파라미터 상수 정의 
 * @desc
 *      Collected constants of general utility
 * @version 0.0.1
 * @date : 2013.07.15
 * @author Suhyun Jeon
 * @author Moonie Song
 */
public class Constants {

    public static final String DRIVER_CF_BASED_USER_BASED_SIMILARITY = "UserBasedSimilarity";

    // for correlation/similarity classes
    public static final String CORR_MANHATTAN = "manhattan";
    public static final String CORR_UCLIDEAN = "uclidean";
    public static final String CORR_COSINE = "cosine";
    public static final String CORR_PEARSON = "pearson";

    // for recommendation
    public static final String RECOM_USER_BASED = "user";
    public static final String MIDTERM_PROCESS_OUTPUT_DIR = "midterm.process.output.dir";

    // etc.
    public static final String UTF8 = "UTF-8";
    public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
}
