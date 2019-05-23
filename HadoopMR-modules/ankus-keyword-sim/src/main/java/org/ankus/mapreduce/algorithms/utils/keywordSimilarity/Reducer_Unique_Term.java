package org.ankus.mapreduce.algorithms.utils.keywordSimilarity;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reducer_Unique_Term extends Reducer<Text, IntWritable, Text, NullWritable>
{
	private Logger logger = LoggerFactory.getLogger(Reducer_Unique_Term.class);
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		
	}
	
	protected void reduce(Text Attribute, Iterable<IntWritable> appears, Context context) throws IOException, InterruptedException
	{
		logger.debug(Attribute.toString());
		int count = 0;
		/*
		Iterator<IntWritable> iterator = appears.iterator();
		while (iterator.hasNext())
        {
			count += iterator.next().get();
        }
		*/
		System.out.println(Attribute.toString());
		context.write(new Text(Attribute),NullWritable.get());
		
	}
}