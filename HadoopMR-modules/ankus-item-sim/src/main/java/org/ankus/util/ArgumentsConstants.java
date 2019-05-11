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
	 * <font face="verdana" color="green">임시 파일 제거인자</font>
	 */
    public static final String TEMP_DELETE = "-tempDelete";
    
    /**
	 * <font face="verdana" color="green">도움말 출력 인자</font>
	 */
    public static final String HELP = "-help";          // current not used, for CLI

    /**
	 * <font face="verdana" color="green">아이템간 유사도에 사용할 알고리즘 옵션 설정 인자</font>
	 */

    public static final String ALGORITHM_OPTION = "-algorithmOption";
 
    /**
	 * <font face="verdana" color="green">상관 관계를 구할 아이템 하한 수를 설정</font>
	 */
    
    public static final String COMMON_COUNT = "-commonCount";
    
    /**
	 * <font face="verdana" color="green">사용자 식별자 인덱스</font>
	 */
    public static final String UID_INDEX = "-uidIndex";
    
    /**
	 * <font face="verdana" color="green">아이템 식별자 인덱스</font>
	 */
    public static final String IID_INDEX = "-iidIndex";
    
    /**
	 * <font face="verdana" color="green">평점 식별자 인덱스</font>
	 */
    public static final String RATING_INDEX = "-ratingIndex";

    /**
	 * <font face="verdana" color="green">유사도를 구할 특정 아이템 아이디</font>
	 */
    public static final String TARGET_ID = "-targetID";
}
