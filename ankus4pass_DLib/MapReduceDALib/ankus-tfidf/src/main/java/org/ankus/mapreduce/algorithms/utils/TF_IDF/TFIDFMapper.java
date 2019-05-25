package org.ankus.mapreduce.algorithms.utils.TF_IDF;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
 
/**
 * "단어@문서ID\t단어의 갯수/전체 단어의 갯수" 형식의 입력 데이터를 받아
 * Key: 단어, Value : 문서ID=단어의 갯수/전체 단어의 갯수로 출력한다.
 * @auth HongJoong.Shin
 * @date :  2016.12.06
 */
public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
	/**
	 * [입력] 단어@문서ID\t단어의 갯수/전체 단어의 갯수
	 * [출력] Key: 단어, Value : 문서ID=단어의 갯수/전체 단어의 갯수
	 * @auth HongJoong.Shin
	 * @date :  2016.12.06
	 * @parameter LongWritable key 입력 파일의 현재 라인에서의 오프셋
	 * @parameter Text value 단어@문서ID\t단어의 갯수/전체 단어의 갯수
	 * @parameter Context context 하둡 환경 설정 변수
	 * @return
	 */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//    	단어@문서ID\t단어의 갯수/전체 단어의 갯수
        String[] wordAndCounters = value.toString().split("\t");
//      wordAndCounters[0] : 단어@문서ID
//      wordAndCounters[1] : 단어의 갯수/전체 단어의 갯수
        
        String[] wordAndDoc = wordAndCounters[0].split("@");
//      wordAndDoc[0] : 단어
//      wordAndDoc[1] : 문서ID        
        context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
    }
}
