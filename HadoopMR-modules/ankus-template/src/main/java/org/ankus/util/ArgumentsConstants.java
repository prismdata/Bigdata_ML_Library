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

    // common
    public static final String INPUT_PATH = "-input"; //입력 경로 설정 인자
    public static final String OUTPUT_PATH = "-output"; //출력 경로 설정 인자 
    public static final String DELIMITER = "-delimiter"; //구분자 설정 인자 
    public static final String TARGET_INDEX = "-indexList"; //ETL 설정 인자 
    public static final String EXCEPTION_INDEX = "-exceptionIndexList"; //ETC 설정 인자에서 제외할 인덱스(ETL 설정 인자가 -1일 경우에 기술함)
   
    //to etl filter 
    public static final String ETL_T_METHOD = "-etlMethod"; //ETL의 기능 호출 값 인자.
    public static final String ETL_RULE_PATH = "-filterRulePath"; //변환에 사용하는 규칙을 저장한 파일 경로 설정 인자.
    public static final String ETL_RULE = "-filterRule"; //추출 제거에 사용하는 문자열 형태의 규칙을 받는 인자.
    public static final String ETL_REPLACE_RULE = "-ReplaceRule"; //컬럼 교체에 사용할 규칙을 받는 인자.
    public static final String ETL_REPLACE_RULE_PATH = "-ReplaceRulePath"; //컬럼 교체에 사용할 규칙 파일 경로 설정 인자.
    public static final String ETL_NUMERIC_NORM = "-NumericForm";  //정규화 규칙을 받는 인자.
    public static final String ETL_NUMERIC_NORM_RULE_PATH = "-NumericFormFile";//정규화 규칙 파일 경로 설정 인자. 
    public static final String ETL_NUMERIC_SORT_METHOD = "-Sort";    //정렬 방법(오름차순, 내림 차순)을 기술함.
    public static final String ETL_NUMERIC_SORT_TARGET = "-SortTarget";//정렬 대상을 기술함.
    public static final String HELP = "-help";   //도움말 출력을 위한 인자.(CLI 전용)
}
