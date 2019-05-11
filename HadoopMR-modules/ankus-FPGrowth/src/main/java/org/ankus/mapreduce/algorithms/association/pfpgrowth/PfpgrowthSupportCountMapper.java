package org.ankus.mapreduce.algorithms.association.pfpgrowth;

import java.io.IOException;
import java.util.StringTokenizer;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 아이템의 발생 횟수를 계산하기 위한 전처리 단계
 * @author HongJoong.Shin
 * @date 2015.03.26
 * @version 0.0.1
 */
public class PfpgrowthSupportCountMapper extends Mapper<LongWritable , Text, Text, IntWritable>{
	private Logger logger = LoggerFactory.getLogger(PfpgrowthSupportCountMapper.class);
	private final static IntWritable one = new IntWritable(1);
	private Text item = new Text();
		
	/**
	 * 입력 트랜잭션의 각 아이템에 숫자(1)객체를 할당하여 Combiner에 전달한다.
	 * @param  LongWritable key : 입력 스프릿 오프셋
	 * @param  Text value : 입력 스프릿 
	 * @param  Context context  : 하둡 콘텍스트 정보 
	 * @author HongJoong.Shin
	 * @date 2015.03.26
	 * @version 0.0.1
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		String delimiter =  context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
		StringTokenizer itr = new StringTokenizer(value.toString(), delimiter);
		
		while(itr.hasMoreTokens())
		{
			String token = itr.nextToken();
			item.set(token);	
			context.write(item,one);
		}	
		Counter counter = context.getCounter("Pfpgrowth","TRANSACTIONS");
        counter.increment(1);
	}
	
}