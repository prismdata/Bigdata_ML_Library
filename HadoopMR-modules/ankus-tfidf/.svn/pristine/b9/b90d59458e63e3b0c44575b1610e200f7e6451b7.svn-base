package org.ankus.mapreduce.algorithms.utils.TF_IDF;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/**
 * 각 문서마다 "개별 단어의 수"를 카운트
 * @auth HongJoong.Shin
 * @parameter LongWritable key
 * @parameter Text value
 * @parameter Context context
 */
public class WordFrequenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private Logger logger = LoggerFactory.getLogger(WordFrequenceReducer.class);
 
	/*
	 * [입력] Key 단어(문서ID에 중복될 수 있음)@문서ID, Value:{<1>...<1>}
	 * [출력] Key 중복 제거된 단어@문서ID, Value: 단어의 갯수
	 * @auth HongJoong.Shin 
	 * @parameter Text key
	 * @parameter Iterable<IntWritable> values
	 * @parameter Context context
	 */
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
 
        int sum = 0;
        for (IntWritable val : values)
        {
            sum += val.get();
        }
        //write the key and the adjusted value (removing the last comma)
        logger.info(key.toString() + "\t" + sum);
        context.write(key, new IntWritable(sum));
    }
}
