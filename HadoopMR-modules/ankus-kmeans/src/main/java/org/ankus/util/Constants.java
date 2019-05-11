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
 * 알고리즘 실행 상수 정의.
 * Collected constants of general utility
 * @version 0.0.1
 * @date : 2013.07.15
 * @author Suhyun Jeon
 * @author Moonie Song
 */
public class Constants {
	/**
	 * <font face="verdana" color="green">범주 데이터 통계 분석 드라이버 호출명</font>
	 */
    public static final String DRIVER_NOMINAL_STATS = "NominalStatistics";
    /**
	 * <font face="verdana" color="green">수치 데이터 통계 분석 드라이버 호출명</font>
	 */
    public static final String DRIVER_NUMERIC_STATS = "NumericStatistics";
    /**
	 * <font face="verdana" color="green">정규화 데이터 통계 분석 드라이버 호출명</font>
	 */
    public static final String DRIVER_NORMALIZE = "Normalization";
    /**
	 * <font face="verdana" color="green">군집 분석 드라이버 호출명</font>
	 */
    public static final String DRIVER_KMEANS_CLUSTERING = "KMeans";
    /**
	 * <font face="verdana" color="green">수치 데이터 타입 기술자</font>
	 */
    public static String DATATYPE_NUMERIC = "numeric";
    /**
	 * <font face="verdana" color="green">범주 데이터 타입 기술자</font>
	 */
    public static String DATATYPE_NOMINAL = "nominal";
    
    /**
	 * <font face="verdana" color="green">최대 최소 값을 갖는 인자</font>
	 */
    public static String STATS_MINMAX_VALUE = "minMaxValue";
    /**
	 * <font face="verdana" color="green">4분위수중 각 분위의 데이터의 갯수</font>
	 */
    public static String STATS_NUMERIC_QUARTILE_COUNTER = "NUMERIC_STAT_BLOCK_DATA_CNT";

    /**
     * <font face="verdana" color="green">거리 측정시 사용할 척도 인자(멘허튼 거리)</font>
     */
    public static final String CORR_MANHATTAN = "manhattan";
    /**
	 * <font face="verdana" color="green">거리 측정시 사용할 척도 인자(유클리드 거리)</font>
	 */
    public static final String CORR_UCLIDEAN = "uclidean";
    
    /**
   	 * <font face="verdana" color="green">파일 엔코딩 설정 인자</font>
   	 */
    public static final String UTF8 = "UTF-8";
    
}
