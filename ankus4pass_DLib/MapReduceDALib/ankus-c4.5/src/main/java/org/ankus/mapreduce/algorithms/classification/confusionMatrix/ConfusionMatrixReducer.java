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

package org.ankus.mapreduce.algorithms.classification.confusionMatrix;

import org.ankus.mapreduce.algorithms.classification.C45.Reducer_EntropyInfo;
import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * ID3ComputeEntropyReducer
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.12
 * @author Moonie Song
 */
public class ConfusionMatrixReducer extends Reducer<Text, IntWritable, NullWritable, Text>{

    String m_delimiter;
    private Logger logger = LoggerFactory.getLogger(ConfusionMatrixReducer.class);
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");

    }

//	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
        Iterator<IntWritable> iterator = values.iterator();
        int totalCnt = 0;
        while (iterator.hasNext())
        {
            iterator.next();
            totalCnt++;
        }
        logger.info(key.toString() + m_delimiter + totalCnt);
        context.write(NullWritable.get(), new Text(key.toString() + m_delimiter + totalCnt));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
