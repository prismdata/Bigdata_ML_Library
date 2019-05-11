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

package org.ankus.mapreduce.verify.compare;

import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * CompareReducer
 *
 * @author Suhyun Jeon
 * @version 0.1
 * @desc
 *      Compare data set between original data set(ex. movielens) and recommendation result data set.
 *      1	1	3.913	5
 *      1	101	3.474	2
 *      1	105	2.739	2
 * @date : 2013.11.18
 * @author Suhyun Jeon
 */
public class CompareReducer extends Reducer<Text, Text, Text, Text> {

    private String delimiter;

	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.delimiter = configuration.get(Constants.DELIMITER);
    }  

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	Iterator<Text> iterator = values.iterator();

        List<String> recommendResultList = new ArrayList<String>();
        List<String> testDataSetList = new ArrayList<String>();

        List<String> recommendKeyList = new ArrayList<String>();
        List<String> testDataSetKeyList = new ArrayList<String>();

        while (iterator.hasNext()){
            Text record = iterator.next();
            String[] columns = record.toString().split(delimiter);

            if(columns[0].equals(Constants.RECOM_RECOMMENDED)){
                recommendResultList.add(columns[1]);
                recommendKeyList.add(key.toString());
            }else{
                testDataSetList.add(columns[1]);
                testDataSetKeyList.add(key.toString());
            }
        }

        for(int i=0; i<testDataSetList.size(); i++) {
            context.write(key, new Text(testDataSetList.get(i) + "\t" + recommendResultList.get(i)));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}