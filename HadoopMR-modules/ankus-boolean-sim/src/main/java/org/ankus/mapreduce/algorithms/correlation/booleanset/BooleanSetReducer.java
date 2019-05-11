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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ankus.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * BooleanSetReducer
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
 * @return The is between the two input VECTOR boolean dataset..
 * 		   Returns 1 if one 0 or both of the booleans are not {@code 0 or 1}.
 *
 * @version 0.0.1
 * @date : 2013.07.10
 * @author Suhyun Jeon
*/
public class BooleanSetReducer extends Reducer<Text, TextIntegerPairWritableComparable, TextTwoWritableComparable, TextIntegerTwoPairsWritableComparable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<TextIntegerPairWritableComparable> values, Context context) throws IOException, InterruptedException {
       
        Iterator<TextIntegerPairWritableComparable> iterator = values.iterator();
        List<String> uniqueKeyList = new ArrayList<String>();
        List<Integer> valueList = new ArrayList<Integer>();

        while(iterator.hasNext()){
        	TextIntegerPairWritableComparable textIntegerPairWritableComparable = iterator.next();

        	uniqueKeyList.add(textIntegerPairWritableComparable.getText().toString());
        	valueList.add(textIntegerPairWritableComparable.getNumber());
        }

        TextTwoWritableComparable textTwoWritableComparable = null;
        TextIntegerTwoPairsWritableComparable textIntegerPairsWritableComparable = null;

        for(int i=0; i<uniqueKeyList.size(); i++) {
        	for(int j=i+1; j<uniqueKeyList.size(); j++) {
                if(uniqueKeyList.get(i).compareTo(uniqueKeyList.get(j)) > 0){
                    textTwoWritableComparable = new TextTwoWritableComparable(uniqueKeyList.get(j), uniqueKeyList.get(i));
                    textIntegerPairsWritableComparable = new TextIntegerTwoPairsWritableComparable(key.toString(), valueList.get(j), key.toString(), valueList.get(i));
                }else{
                    textTwoWritableComparable = new TextTwoWritableComparable(uniqueKeyList.get(i), uniqueKeyList.get(j));
                    textIntegerPairsWritableComparable = new TextIntegerTwoPairsWritableComparable(key.toString(), valueList.get(i), key.toString(), valueList.get(j));

                }
        		context.write(textTwoWritableComparable, textIntegerPairsWritableComparable);
        	}
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}