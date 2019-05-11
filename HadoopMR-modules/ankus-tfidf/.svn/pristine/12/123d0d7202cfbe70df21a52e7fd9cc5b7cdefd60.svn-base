package org.ankus.mapreduce.algorithms.utils.TF_IDF;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
/**
* WordCountsMapper의 결과를 받아 문서내의 단어의 수와 전체 단어의 갯수를 출력.
 * @auth HongJoong.Shin
 * @date :  2016.12.06
 */
public class WordCountsReducer extends Reducer<Text, Text, Text, Text> {
 	/**
	 * 문서ID와 중복이 제거된 단어, 단어의 갯수를 받아 각 문서마다 개별단어의 갯수와 전체 단어의 갯수를 출력
	 * [입력] Key: 문서ID , Value: { <중복이 제거된 단어=단어의 갯수>..<>}
	 * [출력] Key: 단어@문서ID, Value : 단어의 갯수/전체 단어의 갯수
	 * @auth HongJoong.Shin
	 * @date :  2016.12.06
	 */
    protected void reduce(Text DocumentID, Iterable<Text> UniqueWord_Count, Context context) throws IOException, InterruptedException {
        int sumOfWordsInDocument = 0;
        Map<String, Integer> WordWithCount = new HashMap<String, Integer>();
        for (Text val : UniqueWord_Count){
            String[] WordNCount = val.toString().split("=");//단어=갯수
            String Word = WordNCount[0];
            int Count =  Integer.valueOf(WordNCount[1]);
            WordWithCount.put( Word, Count);//단어, 갯수
            //문서 ID가 가진 단어의 갯수(종류 아님)를 누적함.
            sumOfWordsInDocument += Count;
        }
        for (String Word : WordWithCount.keySet())
        {
        	context.write(new Text(Word + "@" + DocumentID.toString()), new Text(WordWithCount.get(Word) + "/" + sumOfWordsInDocument));
        }
    }
}
