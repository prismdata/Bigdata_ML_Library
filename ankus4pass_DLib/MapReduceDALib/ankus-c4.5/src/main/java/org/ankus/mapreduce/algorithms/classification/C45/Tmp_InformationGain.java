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

package org.ankus.mapreduce.algorithms.classification.C45;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InformationGain
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.13
 * @author Moonie Song
 */
public class Tmp_InformationGain {

    private static double m_log2 = Math.log(2);

    // SLF4J Logging
    private static Logger logger = LoggerFactory.getLogger(Tmp_InformationGain.class);


    public static void main(String args[])
    {
        int sumArr[] = new int[3];
        double igArr[] = new double[3];

        int classDist_0[] = {2,3,0};
        sumArr[0] = 0;
        for(int c: classDist_0) sumArr[0] += c;
        igArr[0] = getInformationValue(classDist_0, sumArr[0]);
        logger.info("IG-1: " + igArr[0]);

        int classDist_1[] = {4,0,0,0,0,0,0,1};
        sumArr[1] = 0;
        for(int c: classDist_1) sumArr[1] += c;
        igArr[1] = getInformationValue(classDist_1, sumArr[1]);
        logger.info("IG-2: " + igArr[1]);

        int classDist_2[] = {3,3,0,3};
        sumArr[2] = 0;
        for(int c: classDist_2) sumArr[2] += c;
        igArr[2] = getInformationValue(classDist_2, sumArr[2]);
        logger.info("IG-3: " + igArr[2]);

        int totSum = 0;
        for(int c: sumArr) totSum += c;
        logger.info("E: " + getEntropy(sumArr, totSum, igArr));

    }

    public static double getInformationValue(int[] classDist, int sum)
    {
        double val = 0.0;

        for(int c: classDist)
        {
            double p = (double)c/(double)sum;
            if(c > 0) val = val + (p * Math.log(p)/m_log2);
        }

        if(val==0) return 0;
        else return val * -1;
    }

    public static double getEntropy(int[] attrSumArr, int totalSum, double[] IGArr)
    {
        double val = 0.0;

        for(int i=0; i<attrSumArr.length; i++)
        {
            logger.info(attrSumArr[i] + " : " + IGArr[i]);
            val = val + ((double)attrSumArr[i] / (double)totalSum * IGArr[i]);
        }

        return val;
    }
}
