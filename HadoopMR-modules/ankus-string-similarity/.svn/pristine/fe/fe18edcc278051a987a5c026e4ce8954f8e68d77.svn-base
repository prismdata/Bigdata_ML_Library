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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ankus.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * StringSetReducer
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Hamming distance 2. Edit distance
 * @version 0.0.1
 * @date : 2013.07.11
 * @author Suhyun Jeon
*/
public class StringSetReducer extends Reducer<Text, TextTwoWritableComparable, TextTwoWritableComparable, TextFourWritableComparable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<TextTwoWritableComparable> values, Context context) throws IOException, InterruptedException {
       
        Iterator<TextTwoWritableComparable> iterator = values.iterator();
        List<String> uniqueKeyList = new ArrayList<String>();
        List<String> valueList = new ArrayList<String>();
        
        while(iterator.hasNext()){
            TextTwoWritableComparable textTwoWritableComparable = iterator.next();
        	
        	uniqueKeyList.add(textTwoWritableComparable.getText1().toString());
        	valueList.add(textTwoWritableComparable.getText2().toString());
        }

        TextTwoWritableComparable textTwoWritableComparable = null;
        TextFourWritableComparable textFourWritableComparable = null;

        for(int i=0; i<uniqueKeyList.size(); i++) {
        	for(int j=i+1; j<uniqueKeyList.size(); j++) {
                if(uniqueKeyList.get(i).compareTo(uniqueKeyList.get(j)) > 0){
                    textTwoWritableComparable = new TextTwoWritableComparable(uniqueKeyList.get(j), uniqueKeyList.get(i));
                    textFourWritableComparable = new TextFourWritableComparable(key.toString(), valueList.get(j), key.toString(), valueList.get(i));
                }else{
                    textTwoWritableComparable = new TextTwoWritableComparable(uniqueKeyList.get(i), uniqueKeyList.get(j));
                    textFourWritableComparable = new TextFourWritableComparable(key.toString(), valueList.get(i), key.toString(), valueList.get(j));
                }

        		context.write(textTwoWritableComparable, textFourWritableComparable);
        	}
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}