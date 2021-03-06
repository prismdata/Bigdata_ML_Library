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
package org.ankus.mapreduce.algorithms.clustering.common;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * 알고리즘 수행을 위한 기본 환경 변수 설정과 군집 구성 정보를 제어함.
 * @author Moonie Song
 */
public class ClusterCommon {
	 /**
     * 군집 알고리즘 수행을 위한 기본 환경 변수를 배열로 리턴한다.
     * @author Moonie Song
     * @param Context context : 하둡 환경 설정 변수
     * @param int clusterIndex : 군집 번호
     * @return String[] : 환경 변수 배열
     * @throws Exception
     * @author Moonie Song
     */
    public static String[] getParametersForPurity(Configuration conf, int clusterIndex) throws Exception
    {
        String params[] = new String [10];

        params[0] = ArgumentsConstants.INPUT_PATH;
        params[1] = conf.get(ArgumentsConstants.INPUT_PATH);

        params[2] = ArgumentsConstants.OUTPUT_PATH;
        params[3] = conf.get(ArgumentsConstants.OUTPUT_PATH);

        params[4] = ArgumentsConstants.DELIMITER;
        params[5] = conf.get(ArgumentsConstants.DELIMITER, "\t");

        params[6] = ArgumentsConstants.TARGET_INDEX;
        params[7] = clusterIndex + "";

        params[8] = ArgumentsConstants.TEMP_DELETE;
        params[9] = "true";

        return params;

    }

    /**
     * 군집 번호가 위치한 인덱스를 가져온다.
     * @param Context context : 하둡 환경 설정 변수
     * @return int : 군집 번호를 가진 변수 인덱스
     * @throws Exception
     * @author Moonie Song
     */
    public static int getClusterIndex(Configuration conf) throws Exception
    {
        String delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

        FileSystem fs = FileSystem.get(conf);

        Path inputPath = new Path(conf.get(ArgumentsConstants.INPUT_PATH));
        inputPath = CommonMethods.findFile(fs, inputPath);

        FSDataInputStream fin = fs.open(inputPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

        int index = br.readLine().split(delimiter).length - 2;

        br.close();
        fin.close();

        return index;
    }
    
    /**
     * 각 군집에 저장된 개체 수를 출력한다.
     * @param Context context : 하둡 환경 설정 변수
     * @param String finalOutputFilePath : 파일 출력 경로
     * @throws Exception
     * @author Moonie Song
     */
    public static void finalPurityGen(Configuration conf, String finalOutputFilePath) throws Exception
    {
        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream fout = fs.create(new Path(finalOutputFilePath), true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
        bw.write("# Clustering Result - Purity" + "\n");
        bw.write("# Cluster Number, Assigned Data Count, Assigned Data Ratio" + "\n");

        Path inputPath = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH));
        FileStatus[] status = fs.listStatus(inputPath);
        for(int i=0; i<status.length; i++)
        {
//         if(!status[i].getPath().toString().contains("part-m-")) continue;
        	
            FSDataInputStream fin = fs.open(status[i].getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
            String readStr;
            while((readStr = br.readLine())!=null)
            {
                bw.write(readStr + "\n");
            }
            br.close();
            fin.close();
        }

        bw.close();
        fout.close();
    }
    /**
     * 각 군집에 저장된 개체 수를 csv로 출력
     * @param Context context : 하둡 환경 설정 변수
     * @param finalOutputFilePath : 파일 출력 경로
     * @throws Exception
     * @author Moonie Song
     */
    public static void finalPurityGen_csv(Configuration conf, String finalOutputFilePath) throws Exception
    {
        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream fout = fs.create(new Path(finalOutputFilePath), true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
        bw.write("# Clustering Result - Purity" + "\n");
        bw.write("# Cluster Number,Assigned Data Count,Assigned Data Ratio" + "\n");

        Path inputPath = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH));
        FileStatus[] status = fs.listStatus(inputPath);
        for(int i=0; i<status.length; i++)
        {
            if(!status[i].getPath().toString().contains("result")) continue;

            FSDataInputStream fin = fs.open(status[i].getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
            String readStr;
            String tmpReadStr="";
            double point_ = Math.pow(10,  3);
            int rc = 0;
            while((readStr = br.readLine())!=null)
            {
            	readStr = readStr.replace("\t", ",");
            	if(rc > 0)
            	{
            		String[] token = readStr.split(",");
                	
                	double dblRate = Double.parseDouble(token[2]); 
                	dblRate = Math.round(dblRate * point_) / point_;
                	tmpReadStr = token[0] + "," + token[1] + "," + dblRate;
                	bw.write(tmpReadStr + "\n");
            	}
            	else
            	{
                	bw.write(readStr + "\n");
            	}
            	rc++;
            }
            br.close();
            fin.close();
        }

        bw.close();
        fout.close();
    }


}
