package org.ankus.mapreduce.algorithms.utils.keywordSimilarity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeywordCosSim_Mapper  extends Mapper<Object, Text, Text, Text>
{
	private Logger logger = LoggerFactory.getLogger(KeywordCosSim_Mapper.class);
	List<String> UniqueWord = new ArrayList<String> ();
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();       
    }
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] WordAppears = value.toString().split("\t");
		
		context.write(new Text("WordSet"), new Text(WordAppears[0] + "\u0003" + WordAppears[1]));
	}
	protected void cleanup(Context context) throws IOException, InterruptedException
    {
		System.gc ();
		System.runFinalization ();
    }
}
