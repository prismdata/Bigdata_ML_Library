package org.ankus.mapreduce.algorithms.utils.DocSimilarity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 각 문서의 TFIDF를 산출하기 위해 중복이 제거된 단어를 출력하는 클래스 
 * @author HongJoong.Shin
 * @date 2016.12.06
 */
public class TF_Mapper  extends Mapper<Object, Text, Text, Text>{
	String m_delimiter;
   
    List<String> term = new ArrayList<String>();
    /**
     * 전체 문서의 모든 단어들을 하나의 리스트에 저장한다.
     * @author HongJoong.Shin
     * @date 2016.12.06
	 * @param Context context : 하둡 환경 설정 변수
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    	
        Configuration conf = context.getConfiguration();
       
        FileSystem fs = FileSystem.get(conf);
		Path path = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_ALLTERM");
		FileStatus[] status = fs.listStatus(path);
		for(int i = 0; i < status.length; i++)
		{
			String eachpath =status[i].getPath().toString();
			if(eachpath.indexOf("part-r") > 0)
			{
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
				String line = "";
				while((line = br.readLine())!= null)
				{
						String [] token = line.split(m_delimiter);
						if(term.contains(token[0])== false)
						{
							term.add(token[0]);
						}
				}
				br.close();
			}
		}	
    }
    /**
     * 입력 문서의 단어를 전체 문서에 존제하는 지 확인하며 출력한다.
     * 만약 전체 문서에는 존재하지 않으나 입력 문서에 존재하면 단어 추출 과정에 오류가 발생한 것.
     * [출력] 중복이 제거된 단어, {1,0}
     * @author HongJoong.Shin
     * @date 2016.12.06
	 * @param Object key : 데이터 오프셋 
	 * @param Text value : 1개 문서(복수의 라인 혹은 1개 라인으로 구성된 문서 내용)
	 * @param Context context: 하둡 환경 설정 변수
     */
	@Override
	protected void map(Object key, Text value, Context context)// throws IOException, InterruptedException
	{
		String[] tokens = value.toString().split(" ");
		try
		{
			for(int t = 0; t < term.size(); t++) //전체 문서의 단어.
			{
				String cterm = term.get(t);
				
				for(int i = 0; i < tokens.length; i++) //현재 문서의 단어 
				{
					if(tokens[i].trim().length() > 0)
					{
						if(cterm.equalsIgnoreCase(tokens[i])== true)
						{
							context.write(new Text(cterm) , new Text("1"));							
						}
						else
						{
							context.write(new Text(cterm) , new Text("0"));
						}
					}
				}
			}
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}
	}
	
}