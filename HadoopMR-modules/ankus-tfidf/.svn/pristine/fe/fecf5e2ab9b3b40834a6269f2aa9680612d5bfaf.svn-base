package org.ankus.mapreduce.algorithms.utils.TF_IDF;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/**
 * WordFrequenceInDocument Creates the index of the words in documents,
 * mapping each of them to their frequency.
 */
/**
 * 단어 중요도 계산을 위한 클래스
 * @author HongJoong.Shin
 * @date :  2016.12.06
 */
public class TFIDF_Driver extends Configured implements Tool {
 
    // where to put the data in hdfs when we're done
    private static final String OUTPUT_PATH = "1-word-freq";
 
    // where to read the data from.
    long endTime = 0;
   	long lTime  = 0;
   	long startTime = 0 ; 
    private Logger logger = LoggerFactory.getLogger(TFIDF_Driver.class);
    /**
     * ToolRunner에서 호출되는 실제 알고리즘 시작 함수.
     * @auth HongJoong.Shin
     * @parameter String[] args : 단어 중요도  분석 알고리즘 수행 인자.
     * @return int
     */
    @SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        if(!ConfigurationVariable.setFromArguments(args, conf))
		{
			logger.error("MR Job Setting Failed..");
			logger.info("Error: MR Job Setting Failed..: Configuration Failed");
		     return 1;
		}
        startTime = System.currentTimeMillis();
        Job  job = new Job(conf, "Count Numbers of Documents");
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
        
        //문서의 갯수를 파악하기 위해 입력 파일의 라인수를 카운트 한다.
		job.setJobName("****JOB - Count Numbers of Documents");
		job.setJarByClass(TFIDF_Driver.class);
		job.setMapperClass(Mapper_LineCount.class);
		job.setReducerClass(Reducer_LineCount.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/LC"));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/LC"));
        if(!job.waitForCompletion(true))
        {
            logger.info("Error: Get Terms is not Completeion");
            return 1;
        }
        long documents = job.getCounters().findCounter("Counter", "line").getValue();
        logger.info("Documents: " + documents );
   
//        각 문서마다 단어의 갯수를 구한다.
//        [입력] 문서ID\t 문서
//        [출력]  Key 중복 제거된 단어@문서ID, Value: 단어의 갯수
        job = new Job(conf, "Word Frequence In Document");
        String keywordpath = conf.get(ArgumentsConstants.TF_IDF_KEYWORD_PATH, "");
		DistributedCache.addCacheFile(new URI(keywordpath), job.getConfiguration());
        job.setJarByClass(TFIDF_Driver.class);
        job.setMapperClass(WordFrequenceMapper.class);
        job.setReducerClass(WordFrequenceReducer.class);
        job.setCombinerClass(WordFrequenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.INPUT_PATH));
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/WF"), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/WF"));
        
        if(!job.waitForCompletion(true))
        {
            logger.info("Error: Get Terms is not Completeion");
            return 1;
        }
        
//     [입력] 중복 제거된 단어@문서ID\t단어의 갯수
//     [출력] 단어@문서ID\t단어의 갯수/전체 단어의 갯수
        job = new Job(conf, "WordCount");
        job.setJarByClass(TFIDF_Driver.class);
        job.setMapperClass(WordCountsMapper.class);
        job.setReducerClass(WordCountsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); 
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.OUTPUT_PATH)+ "/WF");
       
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/WC"), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/WC"));
		if(!job.waitForCompletion(true))
        {
            logger.info("Error: WordCountForDoc");
            return 1;
        }
//	     [입력] 단어@문서ID\t단어의 갯수/전체 단어의 갯수
//	     [출력] 단어\t단어가 발견된 문서의 갯수
		job = new Job(conf, "TF_IDF STEP1-DocumentCountByKey");
		job.setJobName("TF_IDF STEP1-DocumentCountByKey");
		job.setJarByClass(TFIDF_Driver.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducerStep1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.OUTPUT_PATH) + "/WC");        
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/DocCountKay"), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/DocCountKay"));
        
        if(!job.waitForCompletion(true))
        {
            logger.info("Error: WordCountForDoc");
            return 1;
        }
        
        conf.setLong("NUMBERSOFDOCUMENT", documents);
        job = new Job(conf, "TF_IDF STEP2-DocumentCountByKey");
        FileSystem fs = null;
        Path path;
    	FileStatus[] status;
        fs = FileSystem.get(conf);
		path = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/DocCountKay");
		status = fs.listStatus(path);
		for (int i=0;i<status.length;i++){
			Path ipath = status[i].getPath();
			logger.info("FList path : " + ipath.getName());
			if(ipath.getName().indexOf("part-r") >= 0)
			{
				logger.info("CacheFile:"+ conf.get(ArgumentsConstants.OUTPUT_PATH) + "/DocCountKay" + "/"+ipath.getName());
				String strpath = conf.get(ArgumentsConstants.OUTPUT_PATH) + "/DocCountKay" + "/"+ipath.getName();
				DistributedCache.addCacheFile(new Path(strpath).toUri(), job.getConfiguration());
			}
		}
		
//		[입력1] 단어@문서ID\t단어의 갯수/전체 단어의 갯수
//		[입력2]  단어\t단어가 발견된 문서의 갯수
//		[출력] Term, Document ID \t tf,log10(idf),tfidf, idf / 전체 문서의 갯수, term의 갯수 / 문서내 전체 term의 갯수.
//		[출력-문서 ID 파일]  tf,log10(idf),tfidf, idf / 전체 문서의 갯수, term의 갯수 / 문서내 전체 term의 갯수.
		job.setJarByClass(TFIDF_Driver.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducerStep2.class);
        job.setNumReduceTasks(20);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); 
        FileInputFormat.addInputPaths(job, conf.get(ArgumentsConstants.OUTPUT_PATH) + "/WC");
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/Raw"), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/Raw"));
        if(!job.waitForCompletion(true))
        {
            logger.info("Error: WordCountForDoc");
            return 1;
        }
        
        
//      TF-IDF값에 따라 내림 차순으로 정렬.
        job = new Job(conf, "TF-IDF Sort");
        job.setJobName("****JOB - TF-IDF Sort");
        
        /**
    	 * [입력] Word, DocumentID
    	 * [출력] tf, log(idf) , tf,idf,  Debug Info....
    	 * TFIDF가 계산된 파일을 로드하여 TFIDF의 내림차순으로 정렬한다.
    	 */
        FileInputFormat.addInputPaths(job,conf.get(ArgumentsConstants.OUTPUT_PATH)+"/Raw/");
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/FinalResult"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) +"/FinalResult"));
        job.setJarByClass(TFIDF_Driver.class);
        job.setMapperClass(TF_IDF_SORT_Map.class);
        job.setReducerClass(TF_IDF_SORT_Reduce.class);
        job.setSortComparatorClass(sortComparator.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        if(!job.waitForCompletion(true))
        {
            logger.info("Error: TF-IDF Sort Split is not Completeion");
            return 1;
        }
        FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/FinalResult"), true);
        endTime = System.currentTimeMillis();
		lTime = endTime - startTime;
		
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/LC"));
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/WF"), true);
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/WC"), true);
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/DocCountKay"), true);
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/Raw"), true);
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/FinalResult"));
		
		System.out.println("Training Finished TIME(sec):" + lTime/1000.0f +"초");
		return 0;
    }
    /**
     * main()함수로 ToolRunner를 사용하여 컨텐츠 기반 유사도를 호출한다.
     * @auth 
     * @parameter String[] args : 유사도 분석 알고리즘 수행 인자.
     * @return
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TFIDF_Driver(), args);
        System.exit(res);
    }
}
