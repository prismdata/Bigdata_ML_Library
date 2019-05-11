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

package org.ankus.mapreduce.algorithms.classification.MultilayerPerceptron;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

//import org.ankus.mapreduce.algorithms.classification.knn.DistClassInfo;
//import org.ankus.mapreduce.algorithms.classification.knn.kNNGlobalNNExtractReducer;
import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MLP_NomalizeNumericCombiner extends Reducer<Text, Text, Text, Text>{
	 String m_delimiter;
	 @Override
	    protected void setup(Context context)
	            throws IOException, InterruptedException
	    {
		    Configuration conf = context.getConfiguration();

	        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
	    }

//		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
	       
		}



	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException
	    {
	    }
}
