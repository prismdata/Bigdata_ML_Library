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

import java.util.Arrays;

import org.ankus.mapreduce.algorithms.recommendation.similarity.Userbased.UserBasedSimilarityDriver;
import org.ankus.util.Constants;
import org.apache.hadoop.util.ProgramDriver;


/**
 * 알고리즘 시작 클래스 등록.
 * A description of an map/reduce program based on its class and a human-readable description.
 * @desc
 *      Collected constants of general utility
 * @version 0.0.1
 * @date : 2013.07.02
 * @update : 2013.10.7
 * @author Suhyun Jeon
 * @author Moonie Song
 */

public class AnkusDriver {

    public static void main(String[] args) {

        ProgramDriver programDriver = new ProgramDriver();
        try
        {
        	programDriver.addClass(Constants.DRIVER_CF_BASED_USER_BASED_SIMILARITY, UserBasedSimilarityDriver.class, "User/Item Collaborative Filtering based Similarity");
            programDriver.driver(args);
            
        	// Success
        	System.exit(0);

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}