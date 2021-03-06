
/* Copyright (C) 2011 ankus (http://www.openankus.org).
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

package org.ankus.mapreduce;

import org.ankus.mapreduce.algorithms.clustering.kmeans.KMeansDriver;
import org.ankus.util.Constants;
import org.apache.hadoop.util.ProgramDriver;


/**
 * 알고리즘 시작 클래스 등록.
 * @version 0.0.1
 * @date : 2013.07.02
 * @update : 2013.10.7
 * @author Suhyun Jeon
 * @author Moonie Song
 */

public class AnkusDriver {
	/**
	 * kMeans 알고리즘을 Program Driver에 등록하는 메인 함수 
	 * @author Suhyun Jeon
	 * @author Moonie Song
	 * @parameter String[] args : 실행 인자 
	 * @date : 2013.07.02
	 * @version 0.0.1
	 */
    public static void main(String[] args) {

        ProgramDriver programDriver = new ProgramDriver();
        try
        {
            programDriver.addClass(Constants.DRIVER_KMEANS_CLUSTERING, KMeansDriver.class, "K-means clustering Algorithm");
            programDriver.driver(args);
        	// Success
        	System.exit(0);

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}