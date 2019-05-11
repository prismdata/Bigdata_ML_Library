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

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * NumericStats1_2MRMergeMapper
 * @desc 2nd mapper class for numeric statistics computation mr job (2-step)
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats2_QuartilesMapper extends Mapper<Object, Text, Text, Text>{

    private String m_delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        m_delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String tokens[] = value.toString().split(m_delimiter);
		context.write(new Text(tokens[0]), new Text(tokens[1]));
	}


    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException
    {

    }

}
