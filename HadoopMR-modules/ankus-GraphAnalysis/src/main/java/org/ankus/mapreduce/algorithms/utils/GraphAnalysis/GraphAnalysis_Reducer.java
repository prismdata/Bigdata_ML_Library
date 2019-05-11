package org.ankus.mapreduce.algorithms.utils.GraphAnalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * GraphLoad_Mapper로 부터 시작 노드와 이웃노드(거리1)을 받아 이웃의 노드의 갯수와 노드 리스트를 출력한다.
 * @author  HongJoong.Shin
 * @date 2016.xx.xx
 */
public class GraphAnalysis_Reducer extends Reducer<Text, Text, Text, Text>
{
		
	/**
	 * 
	 * @author HongJoong.Shin
	 * @date 2016.xx.xx
	 * @param Text startnode : 시작 노드
	 * @param Iterable<Text> edge : 인접 노드 리스트 
	 * @param Context context  :  Job 설정변수
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	protected void reduce(Text startnode, Iterable<Text> edge, Context context) 
	{
		double count = 0;		
		try
		{
			Iterator<Text> iterator = edge.iterator();
			List<String> neighber = new ArrayList<String>();
			while (iterator.hasNext())
	        {
				String end_node = iterator.next().toString();
				neighber.add(end_node);
				count += 1.0;			
	        }
			String neighbors = neighber.toString();
			context.write(startnode, new Text(count+"@@"+neighbors));
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}
	}
	
}
