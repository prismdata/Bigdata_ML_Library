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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ankus.io.TextDoublePairWritableComparable;
import org.ankus.io.TextDoubleTwoPairsWritableComparable;
import org.ankus.io.TextTwoWritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NumericSetReducer
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Cosine coefficient 2. Pearson coefficient 3. Tanimoto coefficient
 *      4. Manhattan distance 5. Uclidean distance
 *       Required data items : allow only numeric values
 * @version 0.0.1
 * @date : 2013.07.11
 * @author Suhyun Jeon
*/
public class NumericSetReducer extends Reducer<Text, TextDoublePairWritableComparable, TextTwoWritableComparable, TextDoubleTwoPairsWritableComparable> {
      
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<TextDoublePairWritableComparable> values, Context context) throws IOException, InterruptedException {
    	
    	Iterator<TextDoublePairWritableComparable> iterator = values.iterator();
    	List<String> uniqueKeyList = new ArrayList<String>();
    	List<Double> valueList = new ArrayList<Double>();
              
        while (iterator.hasNext()) {
            TextDoublePairWritableComparable textDoublePairWritableComparable = iterator.next();

        	uniqueKeyList.add(textDoublePairWritableComparable.getText().toString());
        	valueList.add(textDoublePairWritableComparable.getNumber());
        }

        TextTwoWritableComparable textTwoWritableComparable = null;
        TextDoubleTwoPairsWritableComparable textDoubleTwoPairsWritableComparable = null;

        for(int i=0; i<valueList.size(); i++) {
        	for(int j=i+1; j<valueList.size(); j++) {
                if(uniqueKeyList.get(i).compareTo(uniqueKeyList.get(j)) > 0){
                    textTwoWritableComparable = new TextTwoWritableComparable(uniqueKeyList.get(j), uniqueKeyList.get(i));
                    textDoubleTwoPairsWritableComparable = new TextDoubleTwoPairsWritableComparable(key.toString(), valueList.get(j), key.toString(), valueList.get(i));
                }else{
                    textTwoWritableComparable = new TextTwoWritableComparable(uniqueKeyList.get(i), uniqueKeyList.get(j));
                    textDoubleTwoPairsWritableComparable = new TextDoubleTwoPairsWritableComparable(key.toString(), valueList.get(i), key.toString(), valueList.get(j));
                }
                context.write(textTwoWritableComparable, textDoubleTwoPairsWritableComparable);
        	}
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }

}