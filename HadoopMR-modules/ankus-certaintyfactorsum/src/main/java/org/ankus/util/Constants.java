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
 * 드라이버를 호출하는 상수를 정의하는 클래스
 * @version 0.0.1
 * @date : 2013.07.15
 * @author Suhyun Jeon
 * @author Moonie Song
 */
public class Constants {

    //확신도 기반 합을 수행하는 드라이버 호출명
	public static final String DRIVER_CERTAINTYFACTOR_SUM = "CertaintyFactorSUM";
	//범주 통계 분석을 수행하는 드라이버 호출명
	public static final String DRIVER_NOMINAL_STATS = "NominalStatistics";
	//수치 통계 분석을 수행하는 드라이버 호출명
    public static final String DRIVER_NUMERIC_STATS = "NumericStatistics";

    // for common
    public static String COMMON_MAP_OUTPUT_CNT = "mapOutputRecordCnt";

    //수치 통계에 사용하는 변수.
    public static String STATS_NUMERIC_QUARTILE_COUNTER = "NUMERIC_STAT_BLOCK_DATA_CNT";
    public static final String UTF8 = "UTF-8";

}
