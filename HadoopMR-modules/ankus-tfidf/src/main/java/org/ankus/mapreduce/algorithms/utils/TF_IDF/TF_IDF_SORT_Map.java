package org.ankus.mapreduce.algorithms.utils.TF_IDF;


import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;

import java.util.List;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * TFIDF값에 따라 내림 차순으로 정렬하기 위한 데이터 전처리를 관리함.
 * @auth HongJoong.Shin
 * @date :  2016.12.06
 */
public class TF_IDF_SORT_Map  extends Mapper<LongWritable, Text, DoubleWritable, Text>{
	String m_delimiter;
    String integrationPath = null;
	
    List<String> term = new ArrayList<String>();
   
    /**
	 * [입력] Word, DocumentID \t tf, log(idf) , tf,idf,  Debug Info...
	 * [출력] Key: tfidf, Value : DocumentID \t Word\t tf \t idf
	 * @auth HongJoong.Shin
	 * @date :  2016.12.06
	 * @parameter LongWritable key 입력 파일의 현재 라인에서의 오프셋
	 * @parameter Text value Word, DocumentID \t tf, log(idf) , tf,idf,  Debug Info....
	 * @parameter Context context 하둡 환경 설정 변수
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)// throws IOException, InterruptedException
	{
		String[] tokens = value.toString().split("\t"); //default delimiter for mapreduce
		/*
		tokens[0] : Word, DocumentID
		tokens[1] : tf, log(idf) , tf,idf,  Debug Info....
		 */
		try
		{
			String Word_DocumentID = tokens[0];
			String TF_IDF_DebugInfo = tokens[1];
			
			String Word = Word_DocumentID.split(",")[0];
			String DocumentID = Word_DocumentID.split(",")[1];
			
			String tf = TF_IDF_DebugInfo.split(",")[0];
			String idf = TF_IDF_DebugInfo.split(",")[1];
			String tf_idf = TF_IDF_DebugInfo.split(",")[2];

		
			DoubleWritable tfidf = new DoubleWritable(Double.parseDouble(tf_idf));
			
			//tfidf, wd, tf, idf
			context.write(tfidf, new Text( DocumentID +"\t" + Word + "\t" + tf + "\t" +idf));
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}
	}
	
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    	
    }
}
