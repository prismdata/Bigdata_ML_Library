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

package org.ankus.mapreduce.algorithms.clustering.canopy;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * KMeansClusterAssignFinalMapper
 * @desc final mapper class for k-means mr job
 *
 * @version 0.0.1
 * @date : 2013.08.22
 * @author Moonie
 */
public class CanopyFinalMapper extends Mapper<Object, Text, NullWritable, Text>{

	String mDelimiter;          // delimiter for attribute separation
	ArrayList<String> mCanopyList = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        mDelimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        mCanopyList = new ArrayList<String>();

        FileStatus[] status = FileSystem.get(conf).listStatus(new Path(conf.get("CANOPY_CENTER")));
        for(int i=0; i<status.length; i++)
        {
            if(!status[i].getPath().toString().contains("part-r-")) continue;

            FSDataInputStream fin = FileSystem.get(conf).open(status[i].getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
            String readStr;
            while((readStr = br.readLine())!=null) mCanopyList.add(readStr);
            br.close();
            fin.close();
        }

    }
	


	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		if(mCanopyList.contains(value.toString()))
            context.write(NullWritable.get(), new Text(value.toString() + mDelimiter + "canopy"));
        else context.write(NullWritable.get(), new Text(value.toString() + mDelimiter + "data"));
	}
	



    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {

    }

}
