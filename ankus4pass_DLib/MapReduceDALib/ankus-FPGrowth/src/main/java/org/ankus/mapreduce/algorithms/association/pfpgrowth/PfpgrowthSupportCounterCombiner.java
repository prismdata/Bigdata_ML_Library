package org.ankus.mapreduce.algorithms.association.pfpgrowth;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 아이템의 발생 횟수를 지역적으로 누적하여 Reducer로 전송하는 클래스.
 * @author HongJoong.Shin
 * @date 2015.03.26
 * @version 0.0.1
 */
public class PfpgrowthSupportCounterCombiner  extends Reducer <Text,IntWritable, Text, IntWritable >
{
	private IntWritable result = new IntWritable();
	private Logger logger = LoggerFactory.getLogger(PfpgrowthSupportCountReducer.class);
	
	double tc=0;
	double support=0;
	double minSup=0;
	
	/**
	 * 아이템의 발생 횟수를 지역적으로 누산하여 Reducer로 전송한다.
	 * @param  Text key : Mapper에서 전송된 키 값
	 * @param  Iterable<IntWritable> : Mapper로 부터 전송되는 Iterable Value 
	 * @param  Context context  : 하둡 콘텍스트 정보 
	 * @author HongJoong.Shin
	 * @date 2015.03.26
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException{
		int sum =0;
		
		for(IntWritable val: values)
		{
			logger.info("CumtomCombiner : key" + key.toString() + " VALUE " + val.get());
			sum += val.get();
		}
		result.set(sum);		
		context.write(key, result);
	
	}
}
