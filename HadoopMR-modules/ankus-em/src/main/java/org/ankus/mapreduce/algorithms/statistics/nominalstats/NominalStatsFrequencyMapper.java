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

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * NominalStatsFrequencyMapper
 * @desc 2nd mapper class for nominal statistics computation mr job (2-step)
 * @version 0.0.1
 * @date : 2013.08.20
 * @author Moonie
 */
public class NominalStatsFrequencyMapper extends Mapper<Object, Text, Text, IntWritable>{
	
	private String delimiter;
    // attribute index for nominal value
	private int index;
	private IntWritable intWritable = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // TODO '\t'을 변수명으로 수정해야 함
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
        index = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX,  "0"));
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] columns = value.toString().split(delimiter);
        context.write(new Text(columns[index]), intWritable);
        /*
        for(int i=0; i<columns.length; i++)
		{
			if(i == index)
			{
				context.write(new Text(columns[i]), intWritable);
			}
		}
		*/
        Counter counter = context.getCounter("NOMINALSTAT","MAPCOUNT");
        counter.increment(1);
	}	

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}