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

package org.ankus.mapreduce.algorithms.preprocessing.discretization;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class EntropyWeightedReducer extends Reducer<Text, Text, Text, Text>{
	
	private MultipleOutputs<Text, Text> mos;
	@Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
		mos = new MultipleOutputs<Text, Text>(context);
    }
	protected void reduce(Text Attribute, Iterable<Text> Atr_Info, Context context) throws IOException, InterruptedException
	{
		Iterator<Text> iterator = Atr_Info.iterator();
		double FullRecords = 0.0, Bean_count = 0.0, Entropy = 0.0;
		double WeightedEntpry = 0.0;
		
		while (iterator.hasNext())
        {
			String str_info = iterator.next().toString();
			
			String[] tokens = str_info.split(":");
			
			Entropy = Double.parseDouble(tokens[1]);
			Bean_count = Double.parseDouble(tokens[2]);
			FullRecords = Double.parseDouble(tokens[3]); //Fair
			
			WeightedEntpry += (Bean_count / FullRecords) * Entropy;
        }
		System.out.println("value:"+ Attribute.toString() + " weight:" + WeightedEntpry +" entropy:" + Entropy);
				 
		mos.write("weightedentropy", new Text(Attribute+""), new Text(WeightedEntpry+""));
	}
	public void cleanup(Context context) throws IOException, InterruptedException{
		
		mos.close();
	}
}
