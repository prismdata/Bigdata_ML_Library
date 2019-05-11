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
	 * <font face="verdana" color="green">가중치</font>
	 */
    public static final String Fuzzy_CMeans_P = "-p";
    
    /**
	 * <font face="verdana" color="green">최대 반복수</font>
	 */
    public static final String Fuzzy_CMeans_MAXITERATION = "-maxIteration";
    
    /**
	 * <font face="verdana" color="green">클러스터 수</font>
	 */
    public static final String K_CNT = "-k";
    
    /**
	 * <font face="verdana" color="green">입력 데이터 경로</font>
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
	 * <font face="verdana" color="green">군집에 사용할 속성 인덱스</font>
	 */
    public static final String TARGET_INDEX = "-indexList";
    
    /**
	 * <font face="verdana" color="green">군집에 제외할 속성 인덱스</font>
	 */
    public static final String EXCEPTION_INDEX = "-exceptionIndexList";
    
    /**
	 * <font face="verdana" color="green">도움말 출력 옵션<cli만 해당></font>
	 */
    public static final String HELP = "-help";          

}
