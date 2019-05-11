package org.ankus.mapreduce.algorithms.utils.DocSimilarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Mapper로 부터 단어와 문서명을 받아 특정 단어를 포함하는 문서의 갯수를 이용하여 IDF를 산출하는 클래스 
 * @author HongJoong.Shin
 * @date :  2016.12.06
 */
public class IDF_Reducer extends Reducer<Text, Text, Text, Text>
{
	int DocSize = 0;
    /**
	 * 파일(문서)의 갯수를 확보한다.
	 * @author HongJoong.Shin
	 * @date :  2016.12.06
	 * @param Context context 하둡 환경 설정 변수
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		
		DocSize  = conf.getInt("FILECOUNT", -1);
		
	}
    /**
	 * Mapper로 부터 단어와 문서명을 받아 특정 단어를 포함하는 문서의 갯수를 이용하여
	 * IDF를 산출한다.
	 * @author HongJoong.Shin
	 * @date :  2016.12.06
	 * @param Text token 명사 단어  
	 * @param Iterable<Text> appears <파일 이름,...,>
	 * @param Context context 하둡 환경 설정 변수
	 */
	protected void reduce(Text token, Iterable<Text> appears, Context context) 
	{
		double count = 0;
		List<String> containsFile = new ArrayList<String>();
		try
		{
			Iterator<Text> iterator = appears.iterator();
			while (iterator.hasNext())
	        {
				String file_name = iterator.next().toString();
				if(containsFile.contains(file_name)==false)
				{
					containsFile.add(file_name);
				}
	        }
			count = (double)containsFile.size();
			double idf = 1+ Math.log((double)DocSize /count);
			
			context.write(new Text(token), new Text(idf+""));
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}
	}
	
	
}
