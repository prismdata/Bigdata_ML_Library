package org.ankus.mapreduce.algorithms.utils.TF_IDF;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.NumberFormat;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Mapper로 부터 내림 차순으로 전송 받은 각 단어의 중요도를 문서 ID 명의 파일에 기록한다.
 * @auth HongJoong.Shin
 * @date :  2016.12.06
 */
public class TF_IDF_SORT_Reduce extends Reducer<DoubleWritable, Text, NullWritable, Text>
{
	MultipleOutputs<NullWritable, Text> mos  = null;
	String sorted_Output = null;
	double tfidf_threshold = 0.0;
	/**
	 * reducer에 필요한 출력 경로와 TFIDF 출력하한 값을 설정한다.
	 * @auth HongJoong.Shin
	 * @date :  2016.12.06
	 * @parameter Context context 하둡 환경 설정 변수
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		sorted_Output = conf.get(ArgumentsConstants.OUTPUT_PATH);
		mos = new MultipleOutputs<NullWritable, Text>(context);
		
		tfidf_threshold = conf.getDouble(ArgumentsConstants.TFIDF_Threshold, 0.0);
	}
	/**
	 * [입력] Key: tfidf, Value : DocumentID \t Word\t tf \t idf...
	 * [출력-file_name] Word , TF, IDF , TFIDF
	 * @auth HongJoong.Shin
	 * @date :  2016.12.06
	 * @parameter DoubleWritable tfidf
	 * @parameter Iterable<Text> wd_tf_idf
	 * @parameter Context context 하둡 환경 설정 변수
	 */
	protected void reduce(DoubleWritable tfidf, Iterable<Text> wd_tf_idf, Context context) 
	{
		try
		{
			Iterator<Text> iterator = wd_tf_idf.iterator();
			while (iterator.hasNext())
	        {
				String TextValue = iterator.next().toString();
				String[] ValeArray =  TextValue.split("\t");
				String file_name = ValeArray[0];
				String Word = ValeArray[1];
				String TF = ValeArray[2];
				String IDF = ValeArray[3];
				NumberFormat formatter = new DecimalFormat("#0.000");     
				if(tfidf.get() >= tfidf_threshold)
				{	
					String stfidf = formatter.format(tfidf.get());
					mos.write(NullWritable.get(), new Text(Word + "," + TF + "," + IDF + "," + stfidf ), sorted_Output + "/" + file_name );
				}
			}
		}catch(Exception e)
		{
			System.out.println(e.toString());
		}
	}
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{		
		mos.close();
	}
}
