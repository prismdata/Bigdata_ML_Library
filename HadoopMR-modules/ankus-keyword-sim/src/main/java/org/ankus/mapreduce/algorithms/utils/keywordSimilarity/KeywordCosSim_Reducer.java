package org.ankus.mapreduce.algorithms.utils.keywordSimilarity;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

//import org.ankus.mapreduce.algorithms.utils.TF_IDF_NEWS.Reducer_Unique_Term;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeywordCosSim_Reducer  extends Reducer<Text, Text, Text, Text>
{
	private Logger logger = LoggerFactory.getLogger(KeywordCosSim_Reducer.class);
	List<String> List_WordDistSrc = new ArrayList<String>();
	List<String> List_WordDistDist = new ArrayList<String>();
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		
	}
	//Mapper의 키가 1개이므로. value를 모두 받아서 분리 처리함.
	@SuppressWarnings("unused")
	protected void reduce(Text Attribute, Iterable<Text> appears, Context context) throws IOException, InterruptedException
	{
		Iterator<Text> iterator = appears.iterator();
		while (iterator.hasNext())
        {
			Text dist = iterator.next();
			List_WordDistSrc.add(dist.toString());
			List_WordDistDist.add(dist.toString());
        }
		
		for(int si = 0; si < List_WordDistSrc.size(); si++)
		{
			for(int di = 0; di < List_WordDistDist.size(); di++)
			{
				if(si != di)
				{
					double molecule = 0.0; //분자.
					double modulator = 0.0; //분모.
					
					double mod_x = 0.0, mod_y =0.0;
					
					String[] srcDocList = List_WordDistSrc.get(si).split("\u0003");
					String[] dstDocList = List_WordDistDist.get(di).split("\u0003");
					String Src_word = srcDocList[0];
					
					String Src_docDistribution = srcDocList[1];
					String[] src_docList = Src_docDistribution.split("\u0002");
					String Dst_word = dstDocList[0];
					
					String Dst_docDistribution = dstDocList[1];
					String[] dst_docList = Dst_docDistribution.split("\u0002");
					//분자 계산.
					if(src_docList.length != dst_docList.length)
					{
						logger.info("오류 문서 수 불일치");
					}
					//분자 계산.
				
					for(int sdi = 0; sdi < src_docList.length; sdi++)
					{
						double countX = Double.parseDouble(src_docList[sdi].split("\u0001")[1]);
						double countY = Double.parseDouble(dst_docList[sdi].split("\u0001")[1]);
				
						molecule += countX * countY;
					}
				
					//분모 계산.
					for(int sdi = 0; sdi < src_docList.length; sdi++)
					{
						double count = Double.parseDouble(src_docList[sdi].split("\u0001")[1]);
						mod_x +=  Math.pow(count, 2);
					}
					for(int ddi = 0; ddi < dst_docList.length; ddi++)
					{
						double count = Double.parseDouble(dst_docList[ddi].split("\u0001")[1]);
						mod_y +=  Math.pow(count, 2);
					}
					
					modulator = Math.sqrt(mod_x) * Math.sqrt(mod_y);
					double sim = molecule / modulator;
					if(sim > 1)
					{
						for(int sdi = 0; sdi < src_docList.length; sdi++)
						{
							double countX = Double.parseDouble(src_docList[sdi].split("\u0001")[1]);
//							System.out.print(countX + ",");
						}
						System.out.println("");
						for(int sdi = 0; sdi < src_docList.length; sdi++)
						{
							double countX = Double.parseDouble(dst_docList[sdi].split("\u0001")[1]);
//							System.out.print(countX + ",");
						}
//						System.out.println("");
					}
					DecimalFormat form = new DecimalFormat("#.###");
					String format_sim = form.format(sim);
//					logger.info(Src_word + " " + Dst_word + " Sim " + format_sim);
					context.write(new Text(Src_word + " " + Dst_word), new Text(format_sim));
				}
			}
		}
	}
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
		List_WordDistSrc = null;
		List_WordDistDist = null;
		System.gc ();
		System.runFinalization ();
    }
}
;