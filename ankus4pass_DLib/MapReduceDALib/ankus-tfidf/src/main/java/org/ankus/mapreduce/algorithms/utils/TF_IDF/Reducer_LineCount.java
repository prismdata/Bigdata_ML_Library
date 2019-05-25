package org.ankus.mapreduce.algorithms.utils.TF_IDF;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.mapred.Reporter;

/**
 * LINES에 할당된 값들을 합하여 전체 문서의 갯수를 산출한다.
 * @auth 
 * @parameter
 * @return
 */
public class Reducer_LineCount extends Reducer<Text, IntWritable, Text, IntWritable>
{
	private Logger logger = LoggerFactory.getLogger(Reducer_LineCount.class);
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		logger.info("setup");
	}
	/**
	 * Map의 결과를 전달 받아 "Line"의 전체 갯수(문서의 수)를 계산하고 Counter-line에 저장한다.
	 * @auth HongJoong Shin
	 * @parameter Text Attribute
	 * @parameter Iterable<IntWritable> values
	 * @parameter Context context
	 * @return 
	 */
	protected void reduce(Text Attribute, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		logger.debug(Attribute.toString());
		int sum = 0;
		IntWritable result = new IntWritable();
		for (IntWritable val : values) 
		{
	  		sum += val.get();
		}
		context.write(new Text("LINE") , new IntWritable(sum));	 
		context.getCounter("Counter", "line").setValue(sum);
	}
}
