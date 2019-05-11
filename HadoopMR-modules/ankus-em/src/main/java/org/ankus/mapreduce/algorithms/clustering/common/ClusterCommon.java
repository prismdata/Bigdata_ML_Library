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
 * Created with IntelliJ IDEA.
 * User: Wonmoon
 * Date: 14. 8. 12
 * Time: 오후 10:46
 * To change this template use File | Settings | File Templates.
 */
public class ClusterCommon {

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
            int lc = 0;
            double point_ = 1000;
            while((readStr = br.readLine())!=null)
            {
            	String tmp_readStr = readStr;
            	if(lc > 0)
            	{
            		String[] tkn = readStr.split(",");
            		double ratio = Double.parseDouble(tkn[2]);
            		ratio = Math.round(ratio * point_) / point_;
            		tmp_readStr = tkn[0] + "," + tkn[1]  + "," + ratio;
            	}
                bw.write(tmp_readStr + "\n");
                lc++;
            }
            br.close();
            fin.close();
        }

        bw.close();
        fout.close();
    }
    
    public static void TotalPurity(Configuration conf, String original, String clustere_result) throws Exception
    {
    	
    	FileSystem fs = FileSystem.get(conf);

        Path cluster_path = new Path(clustere_result);
        FileStatus[] status = fs.listStatus(cluster_path);
        for(int i=0; i<status.length; i++)
        {
        	FSDataInputStream f_org = fs.open(new Path(original));
            FSDataInputStream f_cluster = fs.open(status[i].getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(f_cluster, Constants.UTF8));
            String readStr;
            while((readStr = br.readLine())!=null)
            {
//                bw.write(readStr + "\n");
            }
//            br.close();
//            fin.close();
        }

    }
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
            while((readStr = br.readLine())!=null)
            {
            	readStr = readStr.replace("\t", ",");
                bw.write(readStr + "\n");
            }
            br.close();
            fin.close();
        }

        bw.close();
        fout.close();
    }


}
