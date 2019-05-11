package org.ankus.mapreduce.algorithms.utils.keywordSimilarity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordDistByDoc_Mapper  extends Mapper<Object, Text, Text, Text>
{
	private Logger logger = LoggerFactory.getLogger(WordDistByDoc_Mapper.class);
	List<String> UniqueWord = new ArrayList<String> ();
	
	String m_delimiter = "";
	int m_doc_id_idx = 5;
	 ArirangAnalyzerHandler aah  = null;
	@Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        //한개의 문서를 문서 아이디와 구분할 때 구분자로 나누어지면 0번째 열이 문서의 기본 아이디로 설정함.
        m_doc_id_idx = conf.getInt(ArgumentsConstants.DOC_ID_POSITION, 1);
        try
		{
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(int pi = 0; pi < cacheFiles.length; pi++)
			{
				if(cacheFiles != null && cacheFiles.length > 0)
				{
					String line;
					String[] tokens;
					try
					{
						BufferedReader br = new BufferedReader(new FileReader(cacheFiles[pi].toUri().getPath()));
						while((line  = br.readLine())!= null)
						{
							UniqueWord.add(line);
						}
					}catch(Exception e)
					{
						logger.info(e.toString());
					}
				}
			}
			aah = new ArirangAnalyzerHandler(false);	
			
		}catch(Exception e)
		{
			logger.info(e.toString());
		}
    }
	@Override	
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String documents = value.toString();
		documents = documents.replaceAll("\\[|\\]|\\(|\\)|\\▶|\\ⓒ|\\‘|\\,|\\’|\\.|\\-", " ");
		String match = "[^\uAC00-\uD7A3xfe0-9a-zA-Z\\s]";
		documents = documents.replaceAll(match, " ");		
		//연속 스페이스 제거
		String match2 = "\\s{2,}";
		String doc_id = "";
		documents = documents.replaceAll(match2, " ");
//		documents = documents.replaceAll("\t", " ");
		String[] fields  = documents.split(m_delimiter);
		documents = "";//변수 재활용.
		for(int fi = 0; fi < fields.length; fi++)
		{
			if(fi < m_doc_id_idx)
			{
				doc_id += fields[fi];
			}
			else
			{
				documents += fields[fi] +" ";
			}
		}
		documents = documents.trim();
//		System.out.println("AF Trim:" + documents);
		List <String> setfields = aah.getListKeyWord(documents);
//		System.out.println("AF Trim:" + setfields.toString());
		for(String uWord: UniqueWord)
		{
			boolean notFounded = true;
			for (String str_Token : setfields) 
	        {
				if(uWord.equals(str_Token))
	        	{
					context.write(new Text(uWord) , new Text(doc_id+ "\u0001" + 1));
					notFounded = false;
	        	}
	        }
			if(notFounded == true)
			{
				context.write(new Text(uWord) , new Text(doc_id+ "\u0001" + 0));
			}
		}
	}
	protected void cleanup(Context context) throws IOException, InterruptedException
    {
		UniqueWord = null;
		System.gc ();
		System.runFinalization ();
    }
}
