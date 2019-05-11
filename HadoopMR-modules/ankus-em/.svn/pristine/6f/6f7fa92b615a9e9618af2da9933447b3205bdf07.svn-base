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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * OriginalDataMapper
 *
 * @author Suhyun Jeon
 * @version 0.1
 * @desc : Original movielens data set
 * @date : 2013.11.18
 */
public class OriginalDataMapper extends Mapper<LongWritable, Text, Text, Text> {

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

        String id = columns[0];
        String item = columns[1];
        String rating = columns[2];

        context.write(new Text(id + "\t" + item), new Text(Constants.RECOM_ORIGINAL_DATA + "\t" + rating));

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
