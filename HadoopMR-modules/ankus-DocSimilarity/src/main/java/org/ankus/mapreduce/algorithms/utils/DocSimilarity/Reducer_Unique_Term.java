package org.ankus.mapreduce.algorithms.utils.DocSimilarity;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/**
 * Mapper의 결과를 받아 중복이 제거된 단어를 출력 하는 클래스 
 * @author HongJoong.Shin
 * @date 2016.12.06
 */
public class Reducer_Unique_Term extends Reducer<Text, IntWritable, Text, IntWritable>
{
	
	/**
	 * Mapper의 결과를 받아 중복이 제거된 단어를 출력한다.<p>
	 * [출력] Key : 단어, Value : 1
	 * @author HongJoong.Shin
	 * @param Text Attribute : 중복 제거된 단어
	 * @param Iterable<IntWritable> appears : <1,..1>
	 * @param Context context 하둡 환경 설정 변수
	 */
	@Override
	protected void reduce(Text Attribute, Iterable<IntWritable> appears, Context context) throws IOException, InterruptedException
	{
		int count = 0;
		
		context.write(new Text(Attribute), new IntWritable(count));
	}
}