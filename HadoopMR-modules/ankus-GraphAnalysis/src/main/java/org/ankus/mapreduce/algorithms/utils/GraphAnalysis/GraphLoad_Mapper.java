package org.ankus.mapreduce.algorithms.utils.GraphAnalysis;

import java.io.IOException;
import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 입력 데이터로 부터 시작 노드와 이웃 노드를 추출하여 Reducer로 출력한다.
 * @author  HongJoong.Shin
 * @date 2016.xx.xx
 */
public class GraphLoad_Mapper  extends Mapper<Object, Text, Text, Text>{
	String m_delimiter;
   
	/**
	 * Job설정 변수로 부터 노드 구분자를 획득한다.
	 * @author HongJoong.Shin
	 * @date 2016.xx.xx
	 * @param Context context : Job 설정변수
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    	Configuration conf = context.getConfiguration();
    	m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
    }
    /**
     * 
	 * @author HongJoong.Shin
	 * @date 2016.xx.xx
     * @param Object key : 입력 스프릿 오프셋 
     * @param Text value : 입력 스프릿 
     * @param Context context : Job 설정변수
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
     */
	@Override
	protected void map(Object key, Text value, Context context)// throws IOException, InterruptedException
	{
		if(value.toString().trim().length() > 0)
		{
			String[] edge = value.toString().split(m_delimiter);
			try
			{
				String[] endnodes =edge[1].split(";");
				if(endnodes.length > 1)
				{
					for(int ei = 0; ei < endnodes.length; ei++ )
					{
						System.out.println("Mapper:"+edge[0] +":"+ endnodes[ei]);
						context.write(new Text(edge[0]), new Text(endnodes[ei])); //adjecent nodes
					}
				}
				else
				{
					context.write(new Text(edge[0]), new Text(edge[1])); //deal with one eage
				}
				
			}
			catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
	}
	
}
