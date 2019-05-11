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
import org.ankus.util.Constants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * NominalStatsRatioMapper
 * @desc 1st mapper class for nominal statistics computation mr job (2-step)
 * @version 0.0.1
 * @date : 2013.08.20
 * @author Moonie
 */
public class NominalStatsRatioMapper extends Mapper<Object, Text, NullWritable, Text>{
	
	private String delimiter;
    // total count for frequency ratio
	private long totalMapRecords;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // TODO
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
        totalMapRecords = context.getConfiguration().getLong(Constants.COMMON_MAP_OUTPUT_CNT, 0);
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{		
		String valStr = value.toString();
		long val = Long.parseLong(valStr.split(delimiter)[1]);
		double rate = (double)val / (double) totalMapRecords;

        context.write(NullWritable.get(), new Text(valStr + delimiter + rate));
	}	

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
