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

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.ankus.util.HadoopJopPrepare;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * CompareDriver
 *
 * @author Suhyun Jeon
 * @version 0.1
 * @desc User-based or item-based Collaborative Filtering recommendation algorithms
 * Runs a recommendation job as a series of map/reduce
 * @date : 2013.10.01
 */
public class CompareDriver extends Configured implements Tool {

    private String input = null;
    private String recommendedDataInput = null;
    private String output = null;
    private String delimiter = null;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new CompareDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        initArguments(args);

        Job job = new Job(this.getConf());
        job = HadoopJopPrepare.prepareJob(job, new Path(input), new Path(recommendedDataInput), new Path(output), CompareDriver.class,
                RecommendationResultMapper.class, OriginalDataMapper.class, Text.class, Text.class,
                CompareReducer.class, Text.class, Text.class);

        job.getConfiguration().set(Constants.DELIMITER, delimiter);

        boolean step = job.waitForCompletion(true);
        if (!(step)) return -1;

        return 0;
    }

    private void initArguments(String[] args) {
        try {
            for (int i = 0; i < args.length; ++i) {
                if (ArgumentsConstants.INPUT_PATH.equals(args[i])) {
                    input = args[++i];
                } else if (ArgumentsConstants.RECOMMENDED_DATA_INPUT.equals(args[i])) {
                    recommendedDataInput = args[++i];
                } else if (ArgumentsConstants.OUTPUT_PATH.equals(args[i])) {
                    output = args[++i];
                } else if (ArgumentsConstants.DELIMITER.equals(args[i])) {
                    delimiter = args[++i];
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}