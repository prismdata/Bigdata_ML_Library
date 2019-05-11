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

package org.ankus.mapreduce.algorithms.correlation.numericset;

import java.io.IOException;

import org.ankus.io.TextDoubleTwoPairsWritableComparable;
import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * CalculationNumericSetMapper
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Cosine coefficient 2. Pearson coefficient 3. Tanimoto coefficient
 *      4. Manhattan distance 5. Uclidean distance
 *       Required data items : allow only numeric values
 * @version 0.0.1
 * @date : 2013.07.11
 * @author Suhyun Jeon
*/
public class CalculationNumericSetMapper extends Mapper<LongWritable, Text, TextTwoWritableComparable, TextDoubleTwoPairsWritableComparable> {

    private String delimiter;
  
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.delimiter = configuration.get(Constants.DELIMITER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();
//        String[] columns = row.split(delimiter);
        String[] columns = row.split("\t");
   
        String id1 = columns[0];
        String id2 = columns[1];
        String item = columns[2];
        Double valueID1 = Double.parseDouble(columns[3]);
        Double valueID2 = Double.parseDouble(columns[4]);

        TextTwoWritableComparable textTwoWritableComparable = new TextTwoWritableComparable(id1, id2);
        TextDoubleTwoPairsWritableComparable textDoubleTwoPairsWritableComparable = new TextDoubleTwoPairsWritableComparable(item, valueID1, item, valueID2);

        context.write(textTwoWritableComparable, textDoubleTwoPairsWritableComparable);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}