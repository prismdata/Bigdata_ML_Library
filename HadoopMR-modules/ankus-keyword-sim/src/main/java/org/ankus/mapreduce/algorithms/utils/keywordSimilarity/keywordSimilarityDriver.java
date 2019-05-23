package org.ankus.mapreduce.algorithms.utils.keywordSimilarity;

import java.io.File;
import java.net.URI;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
class sortComparator extends WritableComparator {

	 protected sortComparator() {
	  super(DoubleWritable.class, true);
	  // TODO Auto-generated constructor stub
	 }

	 @Override
	 public int compare(WritableComparable o1, WritableComparable o2) {
	  DoubleWritable k1 = (DoubleWritable) o1;
	  DoubleWritable k2 = (DoubleWritable) o2;
	  int cmp = k1.compareTo(k2);
	  return -1 * cmp;
	 }
}
public class keywordSimilarityDriver extends Configured implements Tool {
	private Logger logger = LoggerFactory.getLogger(keywordSimilarityDriver.class);
	int mb = 1024*1024;
	@SuppressWarnings({ "unused", "deprecation" })
	public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new keywordSimilarityDriver(), args);
        System.exit(res);
	}
	long endTime = 0;
   	long lTime  = 0;
   	long startTime = 0 ; 
   	
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception
	{
		Path path = null;
		FileStatus[] status = null;
		FileSystem fs  = null;
		Configuration conf = this.getConf();
		if(!ConfigurationVariable.setFromArguments(args, conf))
		{
			logger.error("MR Job Setting Failed..");
			logger.info("Error: MR Job Setting Failed..: Configuration Failed");
		     return 1;
		}
		 Runtime runtime = Runtime.getRuntime();
		long milliSeconds = 0;//disable timeout
		
		startTime = System.nanoTime();
		logger.info("****JOB - Extraction Unique Word");
		Job job_getTerm = new Job(this.getConf());	
		job_getTerm.setJobName("****JOB - Extraction Unique Word");
		job_getTerm.setJarByClass(keywordSimilarityDriver.class);
		job_getTerm.setMapperClass(Mapper_Unique_Term.class);
		job_getTerm.setReducerClass(Reducer_Unique_Term.class);
		job_getTerm.setOutputKeyClass(Text.class);
		job_getTerm.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPaths(job_getTerm, conf.get(ArgumentsConstants.INPUT_PATH));
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_ALLTERM"), true);//FOR LOCAL TEST
		FileOutputFormat.setOutputPath(job_getTerm, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_ALLTERM"));
		if(!job_getTerm.waitForCompletion(true))
		{
		    logger.info("Error: Get Terms is not Completeion");
		    return 1;
		}
		job_getTerm = null;
		System.gc ();
		System.runFinalization ();
		
		logger.info("****JOB - Word Distribution by Documents");
		Job job_WordDocDribution = new Job(this.getConf());	
		String Unique_WordPath = conf.get(ArgumentsConstants.OUTPUT_PATH)+"_ALLTERM";
		fs = FileSystem.get(conf);
		status = fs.listStatus(new Path(Unique_WordPath));
		long all_term_file_size = 0;
		for (int i=0;i<status.length;i++){
			Path ipath = status[i].getPath();
			logger.info("FList path : " + ipath.getName());
			if(ipath.getName().indexOf("part-r") >= 0)
			{
				logger.info("CacheFile:"+ Unique_WordPath+ "/"+ipath.getName());
				String strpath = Unique_WordPath+ "/"+ipath.getName();
				
				long fsize = fs.getLength(new Path(strpath));
				all_term_file_size += fsize;
				DistributedCache.addCacheFile(new Path(strpath).toUri(), job_WordDocDribution.getConfiguration());
			}
		}
		if(all_term_file_size <= 0)
		{
			logger.info("Term File is empty");
			return 1;
		}
		String doc_path = conf.get(ArgumentsConstants.INPUT_PATH);
		FileInputFormat.addInputPaths(job_WordDocDribution, doc_path);
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_WDFREQ"));
		FileOutputFormat.setOutputPath(job_WordDocDribution, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_WDFREQ"));
		job_WordDocDribution.setJarByClass(keywordSimilarityDriver.class);
		job_WordDocDribution.setMapperClass(WordDistByDoc_Mapper.class);
		job_WordDocDribution.setReducerClass(WordDistByDoc_Reducer.class);
		job_WordDocDribution.setOutputKeyClass(Text.class);
		job_WordDocDribution.setOutputValueClass(Text.class);
		if(!job_WordDocDribution.waitForCompletion(true))
		{
		    logger.info("Error: JOB - Word Distribution by Documents");
		    logger.error("Error: JOB - Word Distribution by Documents");
		    return 1;
		}
		job_WordDocDribution = null;
		System.gc ();
		System.runFinalization ();
		
		logger.info("****JOB - Word Similarity");
		Job job_WordSimilarity = new Job(this.getConf());
		String distribution_path = conf.get(ArgumentsConstants.OUTPUT_PATH)+"_WDFREQ";
		FileInputFormat.addInputPaths(job_WordSimilarity, distribution_path);
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
		FileOutputFormat.setOutputPath(job_WordSimilarity, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
		job_WordSimilarity.setJarByClass(keywordSimilarityDriver.class);
		job_WordSimilarity.setMapperClass(KeywordCosSim_Mapper.class);
		job_WordSimilarity.setReducerClass(KeywordCosSim_Reducer.class);
		job_WordSimilarity.setOutputKeyClass(Text.class);
		job_WordSimilarity.setOutputValueClass(Text.class);
		if(!job_WordSimilarity.waitForCompletion(true))
		{
			logger.info("Error: JOB - Word Similarity");
			logger.error("Error: JOB - Word Similarity");
			return 1;
		}
		job_WordSimilarity = null;
		System.gc ();
		System.runFinalization();
		
		Job job_Similarity_SORT = new Job(this.getConf());
		FileInputFormat.addInputPaths(job_Similarity_SORT, conf.get(ArgumentsConstants.OUTPUT_PATH));
		status = fs.listStatus(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
		
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/sorted"), true);//FOR LOCAL TEST
		FileOutputFormat.setOutputPath(job_Similarity_SORT, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/sorted"));
		System.out.println("Word Sort");
		job_Similarity_SORT.setJarByClass(keywordSimilarityDriver.class);
		job_Similarity_SORT.setMapperClass(Keyword_SortMapper.class);
		job_Similarity_SORT.setReducerClass(Keyword_SortReducer.class);
		job_Similarity_SORT.setSortComparatorClass(sortComparator.class);
		job_Similarity_SORT.setOutputKeyClass(DoubleWritable.class);
		job_Similarity_SORT.setOutputValueClass(Text.class);
		if(!job_Similarity_SORT.waitForCompletion(true))
		{
		    logger.info("Error: TF_IDF_SORT is not Completeion");
		    return 1;
		}
		job_Similarity_SORT = null;
		endTime = System.nanoTime();
		lTime = endTime - startTime;
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_ALLTERM"), true);
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_WDFREQ"), true);
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/part-r-00000"));
		
		System.out.println("Simillarity Analysis Processing TIME(ms) : " + lTime/1000000.0 + "(ms)");
		System.gc ();
		System.runFinalization ();
		return 0;
	}
}