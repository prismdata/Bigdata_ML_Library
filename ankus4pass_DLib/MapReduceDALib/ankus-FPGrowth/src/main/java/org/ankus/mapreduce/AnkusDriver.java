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

import org.ankus.mapreduce.algorithms.association.pfpgrowth.PfpgrowthDriver;
import org.ankus.util.Constants;
import org.apache.hadoop.util.ProgramDriver;


/**
 * FPGrowth 알고리즘을 등록하는 클래스 
 * @author HongJoong.Shin
 * @date 2017.09.29
 */

public class AnkusDriver {
	/**
	 * FPGrowth 알고리즘을 등록하고 호출하는 클래스
	 * @parameter String[] args : 실행 인자 
	 */
    public static void main(String[] args) {

        ProgramDriver programDriver = new ProgramDriver();
        try
        {
            programDriver.addClass(Constants.DRIVER_PFPGROWTH_ASSOCIATION, PfpgrowthDriver.class, "PFP-growth Algorithm");
            programDriver.driver(args);
        	System.exit(0);

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}