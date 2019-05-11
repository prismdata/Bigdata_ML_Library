package org.ankus.mapreduce.algorithms.utils.TF_IDF;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 전체 라인수 (문서 수)를 구하기 위해 파일의 모든 라인을 읽는다.
 * @auth HongJoong Shin
 * @parameter Object key
 * @parameter Text value
 * @parameter Context context
 * @return
 */
public class Mapper_LineCount extends Mapper<Object, Text, Text, IntWritable>{
	
    int mb = 1024*1024;
    
    private Logger logger = LoggerFactory.getLogger(Mapper_LineCount.class);
    
    /**
     * 1개의 입력 레코드를 읽을 때 마다 Key: Line, Value는 1로 설정하여 출력한다.
     * [입력] 문서ID\t 문서
     * [출력] "Line"\t 1
     * @auth 
     * @parameter
     * @return
     */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String documents = value.toString();
	
		context.write(new Text("LINE") , new IntWritable(1));	
	}
	

}
