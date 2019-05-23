package org.ankus.mapreduce.algorithms.utils.keywordSimilarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Keyword_SortMapper  extends Mapper<Object, Text, DoubleWritable, Text>{
	String m_delimiter;
   // String m_ruleCondition;
  //  int m_indexArr[];
   // int m_numericIndexArr[];
   // int m_exceptionIndexArr[];
   // int m_classIndex;
	private Logger logger = LoggerFactory.getLogger(Keyword_SortMapper.class);
    List<String> term = new ArrayList<String>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    	
        Configuration conf = context.getConfiguration();
        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
    }
	@Override
	protected void map(Object key, Text value, Context context)// throws IOException, InterruptedException
	{
		String[] tokens = value.toString().split("\t"); //default delimiter for mapreduce
		
		try
		{
			String keywords = tokens[0];
			double relation = Double.parseDouble(tokens[1]);
			context.write(new DoubleWritable(relation), new Text(keywords));
		}
		catch(Exception e)
		{
			logger.error(e.toString());
		}
	}
	
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
		logger.info("cleanup");
    }
}