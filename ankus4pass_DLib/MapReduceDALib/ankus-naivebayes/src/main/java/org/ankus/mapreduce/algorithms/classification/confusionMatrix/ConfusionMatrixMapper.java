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

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * ID3AttributeSplitMapper
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.12
 * @author Moonie Song
 */
public class ConfusionMatrixMapper extends Mapper<Object, Text, Text, IntWritable>{

    String m_delimiter;
    IntWritable m_countOne;
    int m_classIndex;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        m_countOne = new IntWritable(1);
        m_classIndex = Integer.parseInt(conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
        
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        System.out.println(filename);
    }

	@Override
	protected void map(Object key, Text value, Context context)// throws IOException, InterruptedException
	{
		try
		{
			String strValue = value.toString();
			
			String[] columns = value.toString().split(m_delimiter);

	        int orgLabelIndex = m_classIndex;
	        int newLabelIndex = columns.length -1;
	        System.out.println(columns[orgLabelIndex]  + " -> " +columns[newLabelIndex] );
	        context.write(new Text(columns[orgLabelIndex] + m_delimiter + columns[newLabelIndex]), m_countOne);
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}
        
	}


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
