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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * NumericStats1_2MRMergeMapper
 * @desc 2nd mapper class for numeric statistics computation mr job (2-step)
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats1_2MRMergeMapper extends Mapper<Object, Text, Text, Text>{

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String valueStr = value.toString();

        // TODO 딜리미터 변수값으로 수정
		int splitIndex = valueStr.indexOf("\t");
		
		String keyStr = valueStr.substring(0, splitIndex);
		keyStr = keyStr.substring(0, keyStr.indexOf("_"));
		
		valueStr = valueStr.substring(splitIndex + 1);
		context.write(new Text(keyStr), new Text(valueStr));
	}


    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException
    {

    }

}
