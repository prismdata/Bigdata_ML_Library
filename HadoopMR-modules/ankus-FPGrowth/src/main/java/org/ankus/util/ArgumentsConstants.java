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
 * @date : 2016.12.6
 * @author HongJoong.Shin
 */
public class ArgumentsConstants {
	
	/**
	 * <font face="verdana" color="green">입력 경로 설정 인자</font>
	 */
    public static final String INPUT_PATH = "-input";
    
    /**
	 * <font face="verdana" color="green">출력 경로 설정 인자</font>
	 */
    public static final String OUTPUT_PATH = "-output";
    /**
	 * <font face="verdana" color="green">구분자 설정 인자</font>
	 */
    public static final String DELIMITER = "-delimiter";
    /**
	 * <font face="verdana" color="green">임시 파일 제거인자</font>
	 */
    public static final String TEMP_DELETE = "-tempDelete";
    
    /**
	 * <font face="verdana" color="green">min supply 설정 인자</font>
	 */
    public static final String AR_MINSUPP = "-minSup"; 
    /**
	 * <font face="verdana" color="green">최대 규칙 길이 설정 인자</font>
	 */
    public static final String AR_MAX_RULE_LENGTH = "-maxRuleLength"; 
    /**
	 * <font face="verdana" color="green">confidence, lift 설정 인자</font>
	 */
    public static final String AR_METRIC_TYPE = "-metricType"; 
    /**
	 * <font face="verdana" color="green">matrix값 설정 인자</font>
	 */
    public static final String AR_METRIC_VALUE = "-metricValue"; 
    /**
	 * <font face="verdana" color="green">규칙의 갯수 설정 인자</font>
	 */
    public static final String AR_RULE_COUNT = "-ruleCount"; 
    /**
	 * <font face="verdana" color="green">규칙에 포함할 아이템 설정 인자</font>
	 */
    public static final String AR_TARGET_ITEM = "-targetItemList";
}
