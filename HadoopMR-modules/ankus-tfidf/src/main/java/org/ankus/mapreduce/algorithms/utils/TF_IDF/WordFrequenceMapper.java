package org.ankus.mapreduce.algorithms.utils.TF_IDF;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ankus.mapreduce.algorithms.utils.TF_IDF.ArirangAnalyzerHandler;
import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/**
 * 각문서의 개별 단어에 정수 1을 할당하여, WordFrequenceReducer에서 단어 수를 계산할 수 있도록 전처리 한다.
 * @auth 
 * @parameter
 * @return
 */
public class WordFrequenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Logger logger = LoggerFactory.getLogger(WordFrequenceMapper.class);
	String m_delimiter = "";
	ArirangAnalyzerHandler aah  = null;
    
	@Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    	Configuration conf = context.getConfiguration();
    	m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
    	String keywordpath = conf.get(ArgumentsConstants.TF_IDF_KEYWORD_PATH, "");
   		if(keywordpath.equals("") == false)
		{
   			aah = new ArirangAnalyzerHandler(true, conf, keywordpath);
		}
   		else
   		{
   			aah = new ArirangAnalyzerHandler(false);	
   		}
    }
	/**
	 * 입력 데이터 (문서)를 받아 문서 ID와 발견된 단어들에 대해 1을 할당한다.
	 * 발견된 단어들은 형태소 분석기의 결과 중 명사를 사용한다.
	 * [입력] Values; 문서ID\t 문서
	 * [출력] Key 단어(문서ID에 중복될 수 있음)@문서ID, Value:{<1>...<1>}
	 * 	@auth HongJoong.Shin
	 * @parameter LongWritable key : 데이터 오프셋 
	 * @parameter Text value : 입력 데이터(문서)
	 * @parameter Context context 하둡 환경 설정 변수
	 */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	System.out.println(value.toString());
    	StringTokenizer frame = null;
    	String inputData = "";
    	String doc_ic = "";
    	int curIdx = 0;
    	
    	frame = new StringTokenizer(value.toString(), "\t" );
    	while(frame.hasMoreElements())
        {
    		if(curIdx == 0)
    		{
    			doc_ic += frame.nextToken();
    		}
    		else
    		{
    			inputData += frame.nextToken();
    		}
    		curIdx++;
    	}
    	if(inputData.length() == 0)
    	{
    		logger.error("TF_IDF Title Document Split Faild");
    		System.exit(1);
    	}
    	
    	//대소문자 구문을 무시하기 위해 대문자로 변환.
		List<String> MDFS_KeyWord = aah.getMDFS_KeyWord(inputData);//형태소 분석기 사용시 
		if(MDFS_KeyWord.size() > 0)
		{
	        for (String str_Token : MDFS_KeyWord) 
	        {
	        	StringBuilder valueBuilder = new StringBuilder();
	        	valueBuilder.append(str_Token.toUpperCase());
	        	valueBuilder.append("@");
	        	valueBuilder.append(doc_ic);
	            context.write(new Text(valueBuilder.toString()), new IntWritable(1));
	        }
		}
    }
}