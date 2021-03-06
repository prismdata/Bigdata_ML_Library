package org.ankus.mapreduce.algorithms.association.pfpgrowth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * PfpgrowthMakeFPTree_Reduce1에서 출력한 Conditional FP-tree 파일을 읽어 Key: Suffix, Value: Prefix를 context에 출력한다.   
 * @author HongJoong.Shin
 * @date 2015.03.26
 * @version 0.0.1
 */
public class PfpgrowthPatternGen_Map extends Mapper<LongWritable , Text, Text, Text>
{
	
	private Logger logger = LoggerFactory.getLogger(PfpgrowthPatternGen_Map.class);
	/**
	 * fpgrowthMakeFPTree_Reduce1에서 출력한 Conditional FP-tree 파일을 읽어 들인다. (경로 /FPtree)
	 * 각 데이터 라인은 기분 구분자(tab)으로 구분되어 있으므로 해당 구분자로 Suffix, Prefix를 분리하여 context에 출력한다.    
	 * Key는 Suffix, Value는 Prefix(Suffix들의 경로)가 된다.
	 * PfpgrowthMakeFPTree_Reduce1에서 출력한 Conditional FP-tree 파일을 읽어 Key: Suffix, Value: Prefix를 context에 출력한다.   
	 * @author HongJoong.Shin
	 * @date   2015.03.26
	 * @param  LongWritable key : 입력 스프릿 오프셋
	 * @param  Text input_branch : 탭으로 부모와 자식이 분리된 Conditional FP-tree 정보 
	 * @param  Context context :하둡 콘텍스트 정보 
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(LongWritable key, Text input_branch, Context context) throws IOException, InterruptedException
			{	
				String[] branchs = input_branch.toString().split("\t");				
				logger.info("TO RULE GEN #KEY:" + branchs[0] + ",VALUE:" + branchs[1]);				
				Text output_key = new Text(branchs[0]);//터미널 노드 확보				
				Text output_value = new Text(branchs[1]);				
				context.write(output_key,  output_value);
			}
}
