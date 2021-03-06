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

/**
 * HadoopJopPrepare
 * @desc
 *      Create a map/reduce Hadoop job. Referenced the Apache Mahout.
 * @return
 *      Job
 * @version 0.0.1
 * @date : 2013.09.09
 * @author Suhyun Jeon
 */
package org.ankus.util;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class HadoopJopPrepare {

    private static final Logger log = LoggerFactory.getLogger(HadoopJopPrepare.class);

    private HadoopJopPrepare() { }

    /**
     * Create a map and reduce Hadoop job.
     * @param job The job {@link org.apache.hadoop.mapreduce.Job} to use
     * @param inputPath The input {@link org.apache.hadoop.fs.Path}
     * @param outputPath The output {@link org.apache.hadoop.fs.Path}
     * @param driver The driver name
     * @param mapper The {@link org.apache.hadoop.mapreduce.Mapper} class to use
     * @param mapperKey The {@link org.apache.hadoop.io.Writable} key class.  If the Mapper is a no-op, this value may be null
     * @param mapperValue The {@link org.apache.hadoop.io.Writable} value class.  If the Mapper is a no-op, this value may be null
     * @param reducer The {@link org.apache.hadoop.mapreduce.Reducer} to use
     * @param reducerKey The reducer key class.
     * @param reducerValue The reducer value class.
     * @return The {@link org.apache.hadoop.mapreduce.Job}.
     * @throws IOException if there is a problem with the io.
     *
     * @see #prepareJob(org.apache.hadoop.mapreduce.Job, org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, Class, Class, Class, Class, Class, Class, Class)
     */
    public static Job prepareJob(Job job,
                                 Path inputPath,
                                 Path outputPath,
                                 Class<? extends Configured> driver,
                                 Class<? extends Mapper> mapper,
                                 Class<? extends Writable> mapperKey,
                                 Class<? extends Writable> mapperValue,
                                 Class<? extends Reducer> reducer,
                                 Class<? extends Writable> reducerKey,
                                 Class<? extends Writable> reducerValue) throws IOException {

        job.setJarByClass(driver);

        job.setMapperClass(mapper);
        job.setReducerClass(reducer);

        job.setMapOutputKeyClass(mapperKey);
        job.setMapOutputValueClass(mapperValue);

        job.setOutputKeyClass(reducerKey);
        job.setOutputValueClass(reducerValue);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    /**
     * Create a map and reduce Hadoop job.
     * @param job The job {@link org.apache.hadoop.mapreduce.Job} to use
     * @param inputPath The input {@link org.apache.hadoop.fs.Path}
     * @param outputPath The output {@link org.apache.hadoop.fs.Path}
     * @param driver The driver name
     * @param mapper The {@link org.apache.hadoop.mapreduce.Mapper} class to use
     * @param mapperKey The {@link org.apache.hadoop.io.Writable} key class.  If the Mapper is a no-op, this value may be null
     * @param mapperValue The {@link org.apache.hadoop.io.Writable} value class.  If the Mapper is a no-op, this value may be null
     * @param combiner
     * @param reducer The {@link org.apache.hadoop.mapreduce.Reducer} to use
     * @param reducerKey The reducer key class.
     * @param reducerValue The reducer value class.
     * @return The {@link org.apache.hadoop.mapreduce.Job}.
     * @throws IOException if there is a problem with the io.
     *
     * @see #prepareJob(org.apache.hadoop.mapreduce.Job, org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, Class, Class, Class, Class, Class, Class, Class)
     */
    public static Job prepareJob(Job job,
                                 Path inputPath,
                                 Path outputPath,
                                 Class<? extends Configured> driver,
                                 Class<? extends Mapper> mapper,
                                 Class<? extends Writable> mapperKey,
                                 Class<? extends Writable> mapperValue,
                                 Class<? extends Reducer> combiner,
                                 Class<? extends Reducer> reducer,
                                 Class<? extends Writable> reducerKey,
                                 Class<? extends Writable> reducerValue) throws IOException {

        job.setJarByClass(driver);

        job.setMapperClass(mapper);
        job.setCombinerClass(combiner);
        job.setReducerClass(reducer);

        job.setMapOutputKeyClass(mapperKey);
        job.setMapOutputValueClass(mapperValue);

        job.setOutputKeyClass(reducerKey);
        job.setOutputValueClass(reducerValue);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }


    /**
     * Create a map and reduce Hadoop job.
     * @param job The job {@link org.apache.hadoop.mapreduce.Job} to use
     * @param multiInputPath1 The input {@link org.apache.hadoop.fs.Path}
     * @param multiInputPath2 The input {@link org.apache.hadoop.fs.Path}
     * @param outputPath The output {@link org.apache.hadoop.fs.Path}
     * @param driver The driver name
     * @param mapper The {@link org.apache.hadoop.mapreduce.Mapper} class to use
     * @param mapperKey The {@link org.apache.hadoop.io.Writable} key class.  If the Mapper is a no-op, this value may be null
     * @param mapperValue The {@link org.apache.hadoop.io.Writable} value class.  If the Mapper is a no-op, this value may be null
     * @return The {@link org.apache.hadoop.mapreduce.Job}.
     * @throws IOException if there is a problem with the IO.
     *
     * @see #prepareJob(org.apache.hadoop.mapreduce.Job, org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, Class, Class, Class, Class)
     */
    public static Job prepareJob(Job job,
                                 Path multiInputPath1,
                                 Path multiInputPath2,
                                 Path outputPath,
                                 Class<? extends Configured> driver,
                                 Class<? extends Mapper> mapper,
                                 Class<? extends Writable> mapperKey,
                                 Class<? extends Writable> mapperValue) throws IOException {

        job.setJarByClass(driver);

        job.setMapperClass(mapper);

        job.setMapOutputKeyClass(mapperKey);
        job.setMapOutputValueClass(mapperValue);

        MultipleInputs.addInputPath(job, multiInputPath1, TextInputFormat.class);
        MultipleInputs.addInputPath(job, multiInputPath2, TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    /**
     * Create a map and reduce Hadoop job.
     * @param job The job {@link org.apache.hadoop.mapreduce.Job} to use
     * @param multiInputPath1 The input {@link org.apache.hadoop.fs.Path}
     * @param multiInputPath2 The input {@link org.apache.hadoop.fs.Path}
     * @param outputPath The output {@link org.apache.hadoop.fs.Path}
     * @param driver The driver name
     * @param mapper1 The {@link org.apache.hadoop.mapreduce.Mapper} class to use
     * @param mapper2 The {@link org.apache.hadoop.mapreduce.Mapper} class to use
     * @param mapperKey The {@link org.apache.hadoop.io.Writable} key class.  If the Mapper is a no-op, this value may be null
     * @param mapperValue The {@link org.apache.hadoop.io.Writable} value class.  If the Mapper is a no-op, this value may be null
     * @param reducer The {@link org.apache.hadoop.mapreduce.Reducer} to use
     * @param reducerKey The reducer key class.
     * @param reducerValue The reducer value class.
     * @return The {@link org.apache.hadoop.mapreduce.Job}.
     * @throws java.io.IOException if there is a problem with the IO.
     *
     * @see #prepareJob(org.apache.hadoop.mapreduce.Job, org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, Class, Class, Class, Class, Class, Class, Class, Class)
     */
    public static Job prepareJob(Job job,
                                 Path multiInputPath1,
                                 Path multiInputPath2,
                                 Path outputPath,
                                 Class<? extends Configured> driver,
                                 Class<? extends Mapper> mapper1,
                                 Class<? extends Mapper> mapper2,
                                 Class<? extends Writable> mapperKey,
                                 Class<? extends Writable> mapperValue,
                                 Class<? extends Reducer> reducer,
                                 Class<? extends Writable> reducerKey,
                                 Class<? extends Writable> reducerValue) throws IOException {

        job.setJarByClass(driver);

        job.setReducerClass(reducer);

        job.setMapOutputKeyClass(mapperKey);
        job.setMapOutputValueClass(mapperValue);

        job.setOutputKeyClass(reducerKey);
        job.setOutputValueClass(reducerValue);

        MultipleInputs.addInputPath(job, multiInputPath1, TextInputFormat.class, mapper1);
        MultipleInputs.addInputPath(job, multiInputPath2, TextInputFormat.class, mapper2);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}

