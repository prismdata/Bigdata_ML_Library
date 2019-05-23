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

package org.ankus.mapreduce.algorithms.classification.id3;

import org.ankus.mapreduce.algorithms.classification.rulestructure.EntropyInfo;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * ID3ComputeEntropyReducer
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.12
 * @author Moonie Song
 */
public class ID3ComputeEntropyReducer extends Reducer<Text, Text, NullWritable, Text>{

    int m_indexArr[];
    int m_numericIndexArr[];
    int m_exceptionIndexArr[];
    int m_classIndex;
    String m_delimiter;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
        m_numericIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NUMERIC_INDEX, "-1"));
        m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
        m_classIndex = Integer.parseInt(conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
    }

	@SuppressWarnings("unchecked")
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
        Iterator<Text> iterator = values.iterator();
        
        HashMap<String, HashMap<String, Integer>> attrClassList = new HashMap<String, HashMap<String, Integer>>();
        ArrayList<String> valueList = new ArrayList<String>();
        while (iterator.hasNext())
        {
            String tokens[] = iterator.next().toString().split(m_delimiter);

            if(attrClassList.containsKey(tokens[0]))
            {
                @SuppressWarnings("rawtypes")
				HashMap classMap = attrClassList.get(tokens[0]);
                if(classMap.containsKey(tokens[1]))
                    classMap.put(tokens[1], (Integer)classMap.get(tokens[1])+1);
                else classMap.put(tokens[1], 1);

                attrClassList.put(tokens[0], classMap);
            }
            else
            {
                HashMap<String, Integer> classMap = new HashMap<String, Integer>();
                classMap.put(tokens[1], 1);
                attrClassList.put(tokens[0], classMap);

                valueList.add(tokens[0]);
            }
        }

        EntropyInfo e = new EntropyInfo();
        e.setValueList(valueList);
        e.computeIGVal(attrClassList);
        // attr-index, entropy, [attr-value, data-cnt, purity, class], ...
        context.write(NullWritable.get(), new Text(key.toString() + m_delimiter + e.toString(m_delimiter)));
        
	}






    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}