package org.ankus.mapreduce.algorithms.utils.TF_IDF;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/**
 * 개별 단어들을 포함하는 문서의 갯수를 카운트 한다. 
 * @auth 
 */
public class TFIDFReducerStep1 extends Reducer<Text, Text, Text, Text> {
 
    private static final DecimalFormat DF = new DecimalFormat("###.####");
    MultipleOutputs<Text, Text> mos  = null;
    private Logger logger = LoggerFactory.getLogger(TFIDFReducerStep1.class);
    Configuration conf = null;
   
    /**
     * TFIDFMapper로 부터 단어, 문서ID를 입력 받아 단어가 발견되는 문서의 갯수를 카운트 한다.
     * [입력] Key: 단어, Value : {<문서ID=단어의 갯수/전체 단어의 갯수>...<>}
     * [출력] Key: 단어, Value : 단어가 발견된 문서의 갯수
     * @auth  HongJoong.Shin
     * @parameter Text key 단어
     * @parameter Iterable<Text> values  {<문서ID=단어의 갯수/전체 단어의 갯수>...<>}
     * @parameter Context context 하둡 환경 설정 변수
     */
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int DocumentCountHasKey = 0;
        Map<String, String> tempFrequencies = new HashMap<String, String>();
        System.out.println("key:" + key.toString());
        for (Text val : values)
        {
            DocumentCountHasKey++; 
        }
        context.write(key, new Text(DocumentCountHasKey+""));
     
    }
    /**
     * Reducer가 입력된 모든 데이터를 처리하면 호출됨.
     * 사용된 힙 메모리가 전체 설정 메모리를 초과하면 Garbage 청소가 수행됨.
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    	
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
