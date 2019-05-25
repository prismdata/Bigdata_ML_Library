package org.ankus.mapreduce.algorithms.utils.TF_IDF;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/**
 * WordsInCorpusTFIDFReducer calculates the number of documents in corpus that a given key occurs and the TF-IDF computation.
 * The total number of D is acquired from the job name<img draggable="false" class="emoji" alt="🙂" src="https://s0.wp.com/wp-content/mu-plugins/wpcom-smileys/twemoji/2/svg/1f642.svg"> It is a dirty hack, but the only way I could communicate the number from
 * the driver.
 * @author Marcello de Sales (marcello.desales@gmail.com)
 */
public class TFIDFReducerStep2 extends Reducer<Text, Text, Text, Text> {
 
    private static final DecimalFormat DF = new DecimalFormat("###.####");
    MultipleOutputs<Text, Text> mos  = null;
    private Logger logger = LoggerFactory.getLogger(TFIDFReducerStep2.class);
    Configuration conf = null;
    String output_path = "";
    FileSystem fs = null;
    
	FileStatus[] status;
	String DOCKEY = "";
	long NumberOfDocuments  = 0;
    
	protected void setup(Context context) throws IOException, InterruptedException
	{
		conf = context.getConfiguration();		
		NumberOfDocuments = conf.getLong("NUMBERSOFDOCUMENT", 0);		
		output_path = conf.get(ArgumentsConstants.OUTPUT_PATH)+"/Raw/";
		mos = new MultipleOutputs<Text, Text>(context);
	}
    /**
     * Key: 단어, Value : 문서ID=단어의 갯수/전체 단어의 갯수
     * TF IDF 최종 계산이 수행됨.
	 * @auth HongJoong.Shin
	 * @date :  2016.12.06
     * @parameter Text key
     * @parameter Iterable<Text> values
     * @parameter Context context
     */
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	int DocumentCountHasKey = 0;
        
        Map<String, String> tempFrequencies = new HashMap<String, String>();
        String strTerm = key.toString();
      
        for (Text val : values)
        {
            String[] KeyCount_WordCount = val.toString().split("=");
            logger.info("Value: " + val.toString());
            
            //문서의 수에 따라 HashMap의 크가가 달라진다.
            String DocID = KeyCount_WordCount[0];
            String keyCountOnDocID = KeyCount_WordCount[1];
        
            String[] wordFrequenceAndTotalWords = keyCountOnDocID.split("/"); 

            double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
            									/ Double.valueOf(wordFrequenceAndTotalWords[1]));

            try
        	{
    			Path[] paths = DistributedCache.getLocalCacheFiles(conf);//Get All Term
        		if(paths == null)
        		{
        			logger.error(this.toString() + "-DistributedCache Path is null");
        			System.exit(1);
        			return;
        		}
        		else
        		{
        			logger.info(this.toString() +"DistributedCache ok");
        		}
                fs = FileSystem.getLocal(conf);
                for(Path p: paths)
                {
                	if(p.getName().indexOf("-r") > 0)
                	{
    	            	FSDataInputStream fin = fs.open(p);
    	                BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
    	                logger.info("Distributed cache read Start");
    	                String line;
    	                while((line = br.readLine())!= null)
    	    			{
    	                  String temp = line.split("\t")[0]; 
    	                  if(temp.equals(key.toString()))
    	                  {
    	                  	DocumentCountHasKey = Integer.parseInt(line.split("\t")[1]);
    	                  	break;
    	                  }
    	    			}
    	                logger.info("Distributed cache read End");
    	                br.close();
    	                fin.close();
                	}
                }
                fs.close();
        	}
        	catch(Exception e)
        	{
        		logger.error(e.toString());
        	}
            
            double idf = (double) NumberOfDocuments / (double) DocumentCountHasKey;
            //전체 문서의 수와 키를 가진 문서의 수가 같으면 TF를  TFIDF로 사용하고
            //다르면 TF * MATH.LOG10(IDF)를 TF를 TFIDF로 사용한다.
            //20170905 : 단어 추출 오류 현상에 의한 수식 오류 발생 가능함.
//            double tfIdf = NumberOfDocuments == DocumentCountHasKey ? tf : tf * Math.log10(idf);
            double tfIdf =  tf * Math.log10(idf);
            
            DocID = DocID.replace(":", "_");
            DocID = DocID.replace(" ", "_");
            String emit_key  = key + "," + DocID;         
            //idf / 전체 문서의 갯수, term의 갯수 / 문서내 전체 term의 갯수.
            String emit_value_detail = DocumentCountHasKey + "/"
            										+ NumberOfDocuments + " , " + wordFrequenceAndTotalWords[0] + "/"
            										+ wordFrequenceAndTotalWords[1] ;
         
            
            String emit_value = DF.format(tf)  + "," +DF.format(Math.log10(idf))+ "," + DF.format(tfIdf) + ","+emit_value_detail;
            //Key : Term, Document ID Value: tf,log10(idf),tfidf, idf / 전체 문서의 갯수, term의 갯수 / 문서내 전체 term의 갯수.            
            String path_mos = output_path + DocID ;
            mos.write(new Text(emit_key), new Text(emit_value), path_mos);
        }
        tempFrequencies = null;
        
        Runtime r = Runtime.getRuntime();
        DecimalFormat format = new DecimalFormat("###,###,###.##");
        long max = r.maxMemory();//JVM이 현재 시스템에 요구 가능한 최대 메모리량, 이 값을 넘으면 OutOfMemory 오류가 발생 합니다.
        long total = r.totalMemory();//JVM이 현재 시스템에 얻어 쓴 메모리의 총량
        long free = r.freeMemory();//JVM이 현재 시스템에 청구하여 사용중인 최대 메모리(total)중에서 사용 가능한 메모리       
        logger.info("Max:" + format.format(max) + ", Total:" + format.format(total) + ", Free:"+format.format(free));
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    	
		mos.close();//Dynamic File Name provider stream close
		int mb = 1024*1024;
		Runtime runtime = Runtime.getRuntime();
		logger.info("Memoery:"+ (runtime.totalMemory() - runtime.freeMemory()));
        if((runtime.totalMemory() - runtime.freeMemory()) / mb > runtime.maxMemory() / mb)
		{
			System.gc ();
			System.runFinalization ();
		}
		System.out.println("cleanup");
    	
    }
}
