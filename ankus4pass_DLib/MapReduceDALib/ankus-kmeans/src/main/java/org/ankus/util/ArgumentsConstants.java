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
 * 알고리즘 수행 인자를 정의.
 * @version 0.0.1
 * @date : 2013.08.23
 * @author Suhyun Jeon
 * @author Moonie Song
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
	 * <font face="verdana" color="green">수치 데이터 인덱스 설정 인자</font>
	 */
    public static final String TARGET_INDEX = "-indexList";
    /**
	 * <font face="verdana" color="green">범주 데이터 인덱스 설정 인자</font>
	 */
    public static final String NOMINAL_INDEX = "-nominalIndexList";    

    /**
	 * <font face="verdana" color="green">예외 데이터 인덱스 설정 인자</font>
	 */
    public static final String EXCEPTION_INDEX = "-exceptionIndexList";
    
    /**
	 * <font face="verdana" color="green">분석 과정에서 발생한 임시 파일 삭제 여부</font>
	 */
    public static final String TEMP_DELETE = "-tempDelete";
    /**
	 * <font face="verdana" color="green">CLI에서 도움말 출력 옵션</font>
	 */
    public static final String HELP = "-help";          
    
    /**
	 * <font face="verdana" color="green">정규화시 대상 변수 이외의 변수도 출력할지 여부 설정</font>
	 */
    public static final String REMAIN_FIELDS = "-remainAllFields";
    
    /**
	 * <font face="verdana" color="green">데이터의 정규화 수행 여부 설정</font>
	 */
    public static final String NORMALIZE = "-normalize";
    /**
	 * <font face="verdana" color="green">군집와 반복 횟수를 설정</font>
	 */
    public static final String MAX_ITERATION = "-maxIteration";
    /**
	 * <font face="verdana" color="green">군집의 갯수를 설정</font>
	 */
    public static final String CLUSTER_COUNT = "-clusterCnt";
    /**
	 * <font face="verdana" color="green">군집 모델 경로</font>
	 */
    public static final String CLUSTER_PATH = "-clusterPath";

    /**
	 * <font face="verdana" color="green">군집의 중심 변화 수렴 상한 선</font>
	 */
    public static final String CLUSTER_TRAINING_CONVERGE = "-convergeRate";

    /**
	 * <font face="verdana" color="green">군집과 개체간 거리 측정 방법 설정</font>
	 */
    public static final String DISTANCE_OPTION = "-distanceOption";
    /**
	 * <font face="verdana" color="green">입력 데이터에 군집 번호 할당 여부 설정</font>
	 */
    public static final String FINAL_RESULT_GENERATION = "-finalResultGen";
    /**
	 * <font face="verdana" color="green">군집 모델 정보 경로</font>
	 */
    public static final String TRAINED_MODEL = "-modelPath";
    
    /**
	 * <font face="verdana" color="green">정규화를 위한 수치 통계 모듈에서 사분위수 추출 단계 사용 여부(1이면 최대, 최소, 평균만 계산) </font>
	 */
    public static final String MR_JOB_STEP = "-mrJobStep";
   
}
