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
    public static final String INPUT_PATH = "-input";//입력 경로 설정 인자
    public static final String OUTPUT_PATH = "-output";//출력 경로 설정 인자 
    public static final String DELIMITER = "-delimiter";//생략 가능하며, 생략시 탭('\t')문자를 기본으로 함
    public static final String TARGET_INDEX = "-indexList";//확신도 계산 인자 
    
    public static final String EXCEPTION_INDEX = "-exceptionIndexList";//설정 인자에서 제외할 인덱스(설정 인자가 -1일 경우에 기술함)
    
    //1단계: 분산처리를 위해 데이터를 n개(hadoop 시스템의 reducer의 개수만큼)의 폴드로 나누고 각각의 데이터 폴 드별로 확신도 합을 산출한다.
    //2단계: 1단계를 통해 n개의 폴드별로 산출된 확신도를 전체 합산하여, 전체 확신도 값을 산출한다.
    public static final String MR_JOB_STEP = "-mrJobStep";   //MR JOB을 1단계로 할지 2단계로 할지의 여부, 생략 가능, 기본은 2단계

    public static final String TEMP_DELETE = "-tempDelete";  //JOb수행 중 발생한 중간 데이터 삭제 설정 인자
    public static final String HELP = "-help";              //도움말 출력 기능으로 CLI에서 사용함
    
    // for certainty factor sum
    public static final String CERTAINTY_FACTOR_MAX = "-cfsumMax";//확신도 합 산출시 적용될 최대 확신도 값
}
