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
/**
 * 아이템 기반 추천 알고리즘을 등록하는 클래스 
 */
package org.ankus.mapreduce;

import org.ankus.mapreduce.algorithms.recommendation.recommender.SimilarityBasedRecommenderDriver;
import org.ankus.mapreduce.algorithms.recommendation.recommender.itembased.ItemSimRecommenderDriver;

import java.util.Arrays;
import org.ankus.util.Constants;
import org.apache.hadoop.util.ProgramDriver;

/**
 * 아이템 기반 추천 알고리즘을 등록하는 클래스 
 * @version 0.0.1
 * @date : 2013.07.02
 * @update : 2013.10.7
 * @author Suhyun Jeon
 * @author Moonie Song
 */
public class AnkusDriver {

	/**
	 * 아이템 기반 추천을 위한 메인 함수 
	 * @author Suhyun Jeon
	 * @author Moonie Song
	 * @parameter String[] args : 실행 인자 
	 * @date : 2013.07.02
	 * @version 0.0.1
	 */
    public static void main(String[] args) {

        ProgramDriver programDriver = new ProgramDriver();
        /*
         * hadoop jar ankus-core2-itembasedRecommend-1.1.0.jar  Recommendation -input /hadoop/input/data/ratings.dat -delimiter :: -uidIndex 0 -iidIndex 1 -ratingIndex 2 -similPath /hadoop/input/data/cf_item_sim -basedType item -similDelimiter :: -similThreshold 0.5 -targetUID 9 -tempDelete false -output /hadoop/output/recom_cf_item9_1
         */
        try
        {
        	programDriver.addClass(Constants.DRIVER_RECOMMENDATION, ItemSimRecommenderDriver.class, "Recommendation driver based on item(contents) similarity");
            programDriver.driver(args);
            
        	// Success
        	System.exit(0);

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
