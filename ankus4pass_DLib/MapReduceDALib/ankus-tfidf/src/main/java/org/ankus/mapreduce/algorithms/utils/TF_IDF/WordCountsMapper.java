package org.ankus.mapreduce.algorithms.utils.TF_IDF;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
/**
 *  각 문서ID가 가지는 각 단어의 갯수를 출력한다.
 * @auth HongJoong.Shin
 * @date :  2016.12.06
 */
public class WordCountsMapper extends Mapper<LongWritable, Text, Text, Text> {
 
	/**
	 * 각 문서ID가 가지는 각 단어의 갯수를 출력한다.
	 * [입력] 중복 제거된 단어@문서ID \t 단어의 갯수
	 * [출력] Key: 문서ID , Value: 중복이 제거된 단어=단어의 갯수
	 * @auth HongJoong.Shin
	 * @parameter LongWritable key : 오프셋
	 * @parameter Text value : 단어@문서ID
	 * @parameter Context context 하둡 환경 설정 변수
	 */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //중복 제거된 단어@문서ID \t 단어의 갯수
    	String[] wordAndDocCounter = value.toString().split("\t");
//    	wordAndDocCounter[0] : 중복 제거된 단어@문서ID
//    	wordAndDocCounter[1] : 단어의 갯수
    	
    	//중복 제거된 단어@문서ID
        String[] wordAndDoc = wordAndDocCounter[0].split("@");
//        wordAndDoc[0] : 중복이 제거된 단어
//        wordAndDoc[1] : 문서ID
//        Key: 문서ID , Value: { <중복이 제거된 단어=단어의 갯수>..<>}
        context.write(new Text(wordAndDoc[1]), new Text(wordAndDoc[0] + "=" + wordAndDocCounter[1]));
    }
}