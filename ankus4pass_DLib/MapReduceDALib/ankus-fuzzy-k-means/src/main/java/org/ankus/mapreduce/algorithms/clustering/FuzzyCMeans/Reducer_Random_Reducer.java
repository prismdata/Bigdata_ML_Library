package org.ankus.mapreduce.algorithms.clustering.FuzzyCMeans;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 전체 레코드에 가중치를 벡터를 할당한다.
 * @author HongJoong.Shin
 * @date 2016.12.06
 */
public class Reducer_Random_Reducer extends Reducer<Text, Text, Text, Text> {
	private Logger logger = LoggerFactory.getLogger(Reducer_Random_Reducer.class);
	
	/**
	 * 각 레코드마다 가중치를 부여한다.
	 * @author HongJoong.Shin
	 * @date 2016.12.06
	 * @param Text Attribute : 속성 값
	 * @param Iterable<Text> Weights : 군집별 가중치 
	 * @param Context context  : 하둡 환경 변수
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	protected void reduce(Text Attribute, Iterable<Text> Weights, Context context) throws IOException, InterruptedException
	{
		String vectors = "";
		
		for (Text Weight : Weights) 
		{
			String WeightStr = Weight.toString();
			context.write(new Text(Attribute), new Text("\u0001" + WeightStr));
		}
		
		
	}
}
