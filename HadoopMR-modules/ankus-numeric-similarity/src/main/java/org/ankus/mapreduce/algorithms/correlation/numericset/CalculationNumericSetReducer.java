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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.ankus.io.TextDoubleTwoPairsWritableComparable;
import org.ankus.io.TextTwoWritableComparable;
import org.ankus.util.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CalculationNumericSetReducer
 * @desc
 *      Here's an version of the similarity coefficient and distance calculation.
 *      1. Cosine coefficient 2. Pearson coefficient 3. Tanimoto coefficient
 *      4. Manhattan distance 5. Uclidean distance
 *       Required data items : allow only numeric values
 * @version 0.0.1
 * @date : 2013.07.11
 * @author Suhyun Jeon
*/
public class CalculationNumericSetReducer extends Reducer<TextTwoWritableComparable, TextDoubleTwoPairsWritableComparable, TextTwoWritableComparable, DoubleWritable> {
      
	private String algorithmOption;

	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.algorithmOption = configuration.get(Constants.ALGORITHM_OPTION);
    }

    @Override
    protected void reduce(TextTwoWritableComparable key, Iterable<TextDoubleTwoPairsWritableComparable> values, Context context) throws IOException, InterruptedException {

        if(algorithmOption.equals(Constants.CORR_COSINE)){
            int docProduct = 0;
            int normItemID1 = 0;
            int normItemID2 = 0;

            for(TextDoubleTwoPairsWritableComparable textDoublePairsWritableComparable : values) {
                docProduct += textDoublePairsWritableComparable.getNumber1() * textDoublePairsWritableComparable.getNumber2();

                normItemID1 += Math.pow(textDoublePairsWritableComparable.getNumber1(), 2);
                normItemID2 += Math.pow(textDoublePairsWritableComparable.getNumber2(), 2);
            }

            double cosineCoefficient = docProduct / (Math.sqrt(normItemID1) * Math.sqrt(normItemID2));
            context.write(key, new DoubleWritable(Double.parseDouble(String.format("%.3f%n", cosineCoefficient))));

        }else if(algorithmOption.equals(Constants.CORR_PEARSON)){
        	double sumID1 = 0.0d;
        	double sumID2 = 0.0d;
        	double squareSumID1 = 0.0d;
        	double squareSumID2 = 0.0d;
        	double totalSumIDs = 0.0d;
        	// PCC(Pearson Correlation Coefficient) variable 
        	double r = 0.0d;
        	int n = 0;

        	for(TextDoubleTwoPairsWritableComparable textDoublePairsWritable : values) {
        		
        		// Count values for sigma(standard deviation)
            	n++;
            	
        		//  Sum of item values for users
        		sumID1 += textDoublePairsWritable.getNumber1();
        		sumID2 += textDoublePairsWritable.getNumber2();
        		
        		// Sum of squares for users
        		squareSumID1 += Math.pow(textDoublePairsWritable.getNumber1(), 2);
        		squareSumID2 += Math.pow(textDoublePairsWritable.getNumber2(), 2);
        		
        		// Calculate sum of times for users
        		totalSumIDs += (textDoublePairsWritable.getNumber1() * textDoublePairsWritable.getNumber2());
        	}

    		// 1. Calculate numerator
    		double numerator = totalSumIDs - ((sumID1 * sumID2) / n);
    		
    		// 2. Calculate each of the denominator user1 and denominator user2
    		double denominatorUserId1 = squareSumID1 - ((Math.pow(sumID1, 2)) / n);
    		double denominatorUserId2 = squareSumID2 - ((Math.pow(sumID2, 2)) / n);
  
    		// 3. Calculate denominator
    		double denominator = Math.sqrt(denominatorUserId1 * denominatorUserId2);
    		                            
    		// 4. Calculate PCC(Pearson Correlation Coefficient)
    		if(denominator == 0) {
    			r = 0.0d;
    		}else{
    			r = numerator / denominator;
    		}

            context.write(key, new DoubleWritable(Double.parseDouble(String.format("%.3f%n", r))));

        }else if(algorithmOption.equals(Constants.CORR_TANIMOTO)){
            double tanimotoCoefficient = 0.0d;

            Map<String, Double> itemID1Map = new HashMap<String, Double>();
            Map<String, Double> itemID2Map = new HashMap<String, Double>();

            for(TextDoubleTwoPairsWritableComparable textDoubleTwoPairsWritableComparable : values) {
        		itemID1Map.put(textDoubleTwoPairsWritableComparable.getText1().toString(), textDoubleTwoPairsWritableComparable.getNumber1());
        		itemID2Map.put(textDoubleTwoPairsWritableComparable.getText2().toString(), textDoubleTwoPairsWritableComparable.getNumber2());
        	}

            Collection<String> intersection = CollectionUtils.intersection(itemID1Map.entrySet(), itemID2Map.entrySet());
            double sumItemsSize = itemID1Map.size() + itemID2Map.size();

        	tanimotoCoefficient = ((float)intersection.size()) / ((float)(sumItemsSize - intersection.size()));
            context.write(key, new DoubleWritable(Double.parseDouble(String.format("%.3f%n", tanimotoCoefficient))));
       		
        }else if(algorithmOption.equals(Constants.CORR_MANHATTAN)){
        	double manhattanDistance = 0.0d;
        	
        	for(TextDoubleTwoPairsWritableComparable textDoublePairsWritable : values) {
                manhattanDistance += Math.abs(textDoublePairsWritable.getNumber1() - textDoublePairsWritable.getNumber2());
        	}
            context.write(key, new DoubleWritable(manhattanDistance));
       		
        }else if(algorithmOption.equals(Constants.CORR_UCLIDEAN)){
        	double sum = 0.0d;
        	double uclideanDistance = 0.0d;
        	
        	for(TextDoubleTwoPairsWritableComparable textDoublePairsWritable : values) {
        		sum += Math.pow((textDoublePairsWritable.getNumber1() - textDoublePairsWritable.getNumber2()), 2);
        	}
        	
            uclideanDistance = Math.sqrt(sum);
            context.write(key, new DoubleWritable(Double.parseDouble(String.format("%.3f%n", uclideanDistance))));
        }
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
