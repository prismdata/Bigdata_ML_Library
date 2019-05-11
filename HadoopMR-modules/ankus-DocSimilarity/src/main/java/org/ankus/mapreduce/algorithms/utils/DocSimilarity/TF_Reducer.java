package org.ankus.mapreduce.algorithms.utils.DocSimilarity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * 각 문서의 TF IDF 를 산출하는 클래스 
 * @author HongJoong.Shin
 * @date 2016.12.06
 */
public class TF_Reducer extends Reducer<Text, Text, Text, Text>
{
	HashMap<String , Double> idf_term = new HashMap<String, Double>();
	double allTerm_length = 0.0;
	int wc = 0;
	
	/**
   	 * Mapper에서 TFIDF를 계산하기 위해 사전 계산된 각 문서의 IDF값을 로드하고, 
   	 * 문서내의 중복 포함 단어의 갯수롤 획득한다. 
	 * @author HongJoong.Shin
	 * @date 2016.12.06
	 * @param Context context : 하둡 환경 설정 변수
     */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_IDF");
		FileStatus[] status = fs.listStatus(path);
		for(int i = 0; i < status.length; i++)
		{
			String eachpath =status[i].getPath().toString();
			if(eachpath.indexOf("part-r") > 0)
			{
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
				String line = "";
				while((line = br.readLine())!= null)
				{
						String[] token = line.split(m_delimiter);
						if(idf_term.containsKey(token[0])== false)
						{
							idf_term.put(token[0], Double.parseDouble(token[1]));
						}
				}
				br.close();
			}
		}
		
		wc = conf.getInt("WC", 0);
	}
	/**
   	 *  TF_Mapper로 부터받은 중복이 제거된 단어를 받아 TF, IDF를 계산한다.
   	 *  [출력] 단어 \t tf:tf값 \t idf:idf 값 \t tfidf: tfidf 값 
	 * @author HongJoong.Shin
	 * @date 2016.12.06
	 * @param Text token : 중복이 제거된 단어.
	 * @param Iterable<Text> appears : <1,1...,1>
	 * @param Context context :  하둡 환경 설정 변수
     */
	protected void reduce(Text token, Iterable<Text> appears, Context context) 
	{
		double count = 0;

		try
		{
			Iterator<Text> iterator = appears.iterator();
			while (iterator.hasNext())
	        {
				String str_TknCount= iterator.next().toString();
				count +=  Double.parseDouble(str_TknCount);
	        }
			double tf = count /  wc;
			if(idf_term.containsKey(token.toString())){
				double idf = idf_term.get(token.toString());
				double tf_idf = tf *idf;
				context.write(new Text(token), new Text("tf:"+ tf + "\tidf:"+ idf +"\ttfidf:"+ tf_idf));
			}
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}
	}

}
