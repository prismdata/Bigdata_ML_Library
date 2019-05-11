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

package org.ankus.mapreduce.algorithms.statistics.numericstats;

import java.io.IOException;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NumericStats1_2MRMergeReducer
 * @desc 2nd reducer class for numeric statistics computation mr job (2-step)
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats1_2MRMergeReducer extends Reducer<Text, Text, NullWritable, Text>{

	private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // TODO
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }


//	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
	{
		long cnt = 0;
		double max = 0;
		double min = 0;
		double sum = 0;
		double harmonic_sum = 0;
		double geometric_sum = 0;
		double square_sum = 0;			
		boolean allPositive = true;
		
		Iterator<Text> iterator = values.iterator();		
		while (iterator.hasNext()) 
        {
			String tokens[] = iterator.next().toString().split(delimiter);
			
			long curCnt = Long.parseLong(tokens[0]);
			double curMax = Double.parseDouble(tokens[1]);
			double curMin = Double.parseDouble(tokens[2]);
			cnt += curCnt;
        	if(cnt==curCnt)
			{
				max = curMax;
				min = curMin;
			}
			else
			{
				if(max < curMax) max = curMax;
				if(min > curMin) min = curMin;
			}
			
			if(tokens[7].equals("F")) allPositive = false;
			sum += Double.parseDouble(tokens[3]);
			if(allPositive)
			{
				harmonic_sum += Double.parseDouble(tokens[4]);
				geometric_sum += Double.parseDouble(tokens[5]);
			}
			square_sum += Double.parseDouble(tokens[6]);
        }
		
		double avg = sum / (double)cnt;
		double avg_harmonic = 0;
		double avg_geometric = 0;
		if(allPositive)
		{
			avg_harmonic = (double)cnt / harmonic_sum;
			avg_geometric = Math.pow(10, geometric_sum /(double)cnt);
		}
		
		double variance = (square_sum * 10000 /(double)cnt) - Math.pow(avg,2);
		double stdDeviation = Math.sqrt(variance);		
		double middleData_Value = (max + min) / 2;
		
		String writeVal = sum + delimiter +
							avg + delimiter +
							avg_harmonic + delimiter +
							avg_geometric + delimiter +
							variance + delimiter +
							stdDeviation + delimiter +
							max + delimiter +
							min + delimiter +
                            cnt;
		context.write(NullWritable.get(), new Text(key.toString() + delimiter + writeVal));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
