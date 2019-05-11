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


package org.ankus.mapreduce;

import org.ankus.mapreduce.algorithms.statistics.nominalstats.NominalStatsDriver;
import java.util.Arrays;
import org.ankus.util.Constants;
import org.apache.hadoop.util.ProgramDriver;


/**
 * A description of an map/reduce program based on its class and a human-readable description.
 * @desc
 *      Collected constants of general utility
 * @version 0.0.1
 * @date : 2013.07.02
 * @update : 2013.10.7
 * @author Suhyun Jeon
 * @author Moonie Song
 * @param tag descriptions.(MAIN DRIVER)
 */

public class AnkusDriver {
	/**
	 * AnkusDriver 생성자: 수행 태스크 없음.
	 */
	public AnkusDriver()
	{
		
	}
	/**
	 * 분석 모듈을 드라이버에 할당한다.
	 */
    public static void main(String[] args) {

        ProgramDriver programDriver = new ProgramDriver();
        //GraphAnalysisDriver
        try
        {

        	programDriver.addClass(Constants.DRIVER_NOMINAL_STATS, NominalStatsDriver.class, "Statistics(frequency/ratio) for Nominal Attributes of Data");
            programDriver.driver(args);
            
        	// Success
        	System.exit(0);

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}