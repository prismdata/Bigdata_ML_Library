package org.ankus.mapreduce.algorithms.utils.keywordSimilarity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Keyword_SortReducer extends Reducer<DoubleWritable, Text, NullWritable, Text>
{
	private Logger logger = LoggerFactory.getLogger(Keyword_SortReducer.class);
	protected void reduce(DoubleWritable similarity, Iterable<Text> keywords, Context context) 
	{
		try
		{
			Iterator<Text> iterator = keywords.iterator();
			while (iterator.hasNext())
	        {
				String wti = iterator.next().toString();
				String[] wd_info = wti.split(" ");
				
				NullWritable out = NullWritable.get();
				if(similarity.get() > 0)
				{
//				logger.info("sorted :" + wd_info[0]+"," + wd_info[1] + ", Similarity," + similarity.toString());
				context.write(out, new Text(wd_info[0]+"," + wd_info[1] + "," + similarity.toString())) ;
				}
			}
		}catch(Exception e)
		{
			logger.error(e.toString());
		}
	}
}