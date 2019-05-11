package org.ankus.mapreduce.algorithms.utils.keywordSimilarity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordDistByDoc_Reducer  extends Reducer<Text, Text, Text, Text>
{
	private Logger logger = LoggerFactory.getLogger(WordDistByDoc_Reducer.class);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		
	}
	protected void reduce(Text Attribute, Iterable<Text> doc_appears, Context context) throws IOException, InterruptedException
	{
		HashMap<String, Long> Doc_DistMap = new HashMap<String, Long>();
	
		Iterator<Text> iterator = doc_appears.iterator();
		while (iterator.hasNext())
        {
			String doc_Value  = iterator.next().toString();
			String[] doc_dist = doc_Value.split("\u0001");
			String dist_key = doc_dist[0];
			long dist_count = Long.parseLong(doc_dist[1]);
			if(Doc_DistMap.containsKey(dist_key))
			{
				long count = Doc_DistMap.get(dist_key);
				Doc_DistMap.put(dist_key, count + dist_count);
			}
			else
			{
				Doc_DistMap.put(dist_key, dist_count);
			}
        }
		String emit_value = "";
		Iterator<String> doc_keys = Doc_DistMap.keySet().iterator();
        while( doc_keys.hasNext())
        {
            String key = doc_keys.next();
            emit_value += key + "\u0001" + Doc_DistMap.get(key) + "\u0002";
        }
        emit_value  = emit_value.substring(0,  emit_value.length()-1);
		//word \t Freq of Doc1, Freq of Doc2, ..., Freq of DocN
		context.write(new Text(Attribute), new Text(emit_value));
		
	}
}
