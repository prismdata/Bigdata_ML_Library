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

package org.ankus.mapreduce.algorithms.correlation.booleanset;

import java.io.IOException;

import org.ankus.io.TextIntegerTwoPairsWritableComparable;
import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * CalculationBooleanSetMapper
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Dice coefficient 2. Jaccard coefficient 3. Hamming distance
 *
 * Example dataset
 * ------------------------
 * 1    0   1   1   1   0
 * 0    0   0   0   1   1
 * 1    0   1   0   1   0
 *
 * @return The is between the two input VECTOR boolean data set.
 * 		   Returns 1 if one 0 or both of the boolean are not {@code 0 or 1}.
 *
 * @version 0.0.1
 * @date : 2013.07.10
 * @author Suhyun Jeon
*/
public class CalculationBooleanSetMapper extends Mapper<LongWritable, Text, TextTwoWritableComparable, TextIntegerTwoPairsWritableComparable> {

    private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.delimiter = configuration.get(Constants.DELIMITER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String row = value.toString();
        String[] columns = row.split(delimiter);

        String id1 = columns[0];
        String id2 = columns[1];
        String item = columns[2];
        Integer valueID1 = Integer.parseInt(columns[3]);
        Integer valueID2 = Integer.parseInt(columns[4]);

        TextTwoWritableComparable textTwoWritableComparable = new TextTwoWritableComparable(id1, id2);
        TextIntegerTwoPairsWritableComparable textIntegerTwoPairsWritableComparable = new TextIntegerTwoPairsWritableComparable(item, valueID1.intValue(), item, valueID2.intValue());

        context.write(textTwoWritableComparable, textIntegerTwoPairsWritableComparable);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}