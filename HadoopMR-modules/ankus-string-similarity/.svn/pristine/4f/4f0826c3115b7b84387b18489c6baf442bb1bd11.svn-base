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

package org.ankus.mapreduce.algorithms.correlation.stringset;

import java.io.IOException;

import org.ankus.io.TextFourWritableComparable;
import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * CalculationStringSetMapper
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Hamming distance 2. Edit distance
 * @version 0.0.1
 * @date : 2013.07.11
 * @author Suhyun Jeon
*/
public class CalculationStringSetMapper extends Mapper<LongWritable, Text, TextTwoWritableComparable, TextFourWritableComparable> {

    private String delimiter;

    //전단계(Reducer)의 출력 데이터가 구분자 탭으로 구성되므로 구분자를 고정으로 변경.-HongJoong.Shin
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
//        this.delimiter = configuration.get(Constants.DELIMITER);
        this.delimiter = "\t";
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String row = value.toString();
        String[] columns = row.split(delimiter);

        String id1 = columns[0];
        String id2 = columns[1];
        String item = columns[2];
        String valueID1 = columns[3];
        String valueID2 = columns[4];

        TextTwoWritableComparable textTwoWritableComparable = new TextTwoWritableComparable(id1, id2);
        TextFourWritableComparable textFourWritableComparable = new TextFourWritableComparable(item, valueID1, item, valueID2);

        context.write(textTwoWritableComparable, textFourWritableComparable);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}