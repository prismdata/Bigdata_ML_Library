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


import org.ankus.mapreduce.algorithms.preprocessing.discretization.EntropyDiscretizationDriver;
import org.ankus.util.Constants;
import org.apache.hadoop.util.ProgramDriver;

/**
 * MDL 알고리즘을 등록하는 클래스 
 * @author HongJoong.Shin
 * @date :  2017.09.29
 */
public class AnkusDriver {

    public static void main(String[] args) {
    	/**
    	 * MDL 알고리즘을 등록하고 호출하는 클래스
    	 * @parameter String[] args : 실행 인자 
    	 */
        ProgramDriver programDriver = new ProgramDriver();
        try
        {
        	programDriver.addClass(Constants.DRIVER_EntropyDISCRETIZATION, EntropyDiscretizationDriver.class, "Numeric Data Discretization");
            programDriver.driver(args);
        	// Success
        	System.exit(0);

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}