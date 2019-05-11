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

package org.ankus.mapreduce.algorithms.statistics.nominalstats;

import java.io.IOException;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NominalStatsFrequencyReducer
 * @desc 1st reducer class for nominal statistics computation mr job (2-step)
 * @version 0.0.1
 * @date : 2013.08.20
 * @author Moonie
 */
public class NominalStatsFrequencyReducer extends Reducer<Text, IntWritable, NullWritable, Text>{
	
	private String delimiter;
    private double totalCnt;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

        totalCnt = (double)context.getCounter("NOMINALSTAT","MAPCOUNT").getValue();

//        Job j = new Job(context.getConfiguration());
//        totalCnt = (double)j.getCounters().findCounter("NOMINALSTAT","MAPCOUNT").getValue();
    }

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		Iterator<IntWritable> iterator = values.iterator();
						
		long sum = 0;
        while (iterator.hasNext()) 
        {
        	sum += iterator.next().get();
        }
        context.write(NullWritable.get(), new Text(key.toString() + delimiter + sum));

//        double ratio = (double)sum / totalCnt;
//        context.write(NullWritable.get(), new Text(key.toString() + delimiter + sum + delimiter + ratio));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
