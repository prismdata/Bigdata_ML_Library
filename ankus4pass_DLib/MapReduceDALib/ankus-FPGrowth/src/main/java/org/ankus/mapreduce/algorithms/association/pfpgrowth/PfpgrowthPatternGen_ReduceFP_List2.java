package org.ankus.mapreduce.algorithms.association.pfpgrowth;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.LinkedHashSet;

import org.ankus.util.ArgumentsConstants;
//import org.apache.commons.math.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * 빈발 패턴과 규칙을 생성한다
 * @author HongJoong.Shin
 * @date 2015.03.26
 * @version 0.0.1
 */
public class PfpgrowthPatternGen_ReduceFP_List2 extends Reducer <Text,Text, Text, Text >
{
	private Logger logger = LoggerFactory.getLogger(PfpgrowthPatternGen_ReduceFP_List2.class);
	
	HashMap<String, HashMap<String, Double>> rule_map_rds = new HashMap<String, HashMap<String, Double>>();
	HashMap<List<String>,  Long > pattern_map_rds = new HashMap<List<String>, Long>();
	
	int rule_length = 0;
	String target_item="";
	
	Configuration conf = null;
	MultipleOutputs<Text, Text> mos;
	/**
	 * 사용자로 입력으로 부터 Parameter로 부터 검색할 데이터(아이템), 최대 규칙 길이를 획득한다.  
	 * @author HongJoong.Shin
	 * @date 2015.03.26
	 * @version 0.0.1
	 * @param  Context context : 하둡 콘텍스트 정보 
	 */
	@Override	
	public void setup(Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();		
		try
		{			
			target_item = conf.get(ArgumentsConstants.AR_TARGET_ITEM, "");			
			
			//최대 규칙 길이 제어 
			rule_length = conf.getInt(ArgumentsConstants.AR_MAX_RULE_LENGTH, Integer.MAX_VALUE);
			if(rule_length < 0)
			{
				rule_length = Integer.MAX_VALUE;
			}
		}
		catch(Exception e)
		{
			logger.error(e.toString());
		}
		
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	/**
	 * 각 Key(Suffix)에 해당하는 Value(Prefix)를 list_PatternBase에 저장
	 * list_PatternBase의 각 prefix마다 발생 수 정보를 제거하여, 부분 집합을 형성하여 각 부분 집합의 최소 반복수를 계산함 
	 * list_PatternBase에서 동일한 부분 집합이 발생하면 누산하고, 신규면 최소 값을 데이터와 함께 Pattern_count에 할당한다.
	 * list_PatternBase에 저장된 각 prefix을 이용하여 suffix가 포함된 부분 집합을 형성하여 발생수와 함께 Frequent Pattern인 Pattern_Map에 저장 
	 * 저장된 Frequent Pattern의 길이가 2이면 LR 규칙을 생성/파일로 출력 한다.
	 * 저장된 Frequent Pattern의 길이가 3이상이면 조합 생성기를 이용하여 LR규칙을 생성/파일로 출력 한다.
	 * @author HongJoong.Shin
	 * @date 2015.03.26
	 * @version 0.0.1
	 * @param  Text terminal : Suffix
	 * @param  Iterable<Text> prefixs : Prefix 리스트
	 * @param  Context context : 하둡 콘텍스트 정보
	 */
	@SuppressWarnings("unchecked")
	public void reduce(Text terminal, Iterable<Text> prefixs, Context context)throws IOException, InterruptedException
	{
		String[] node_name = null;
		HashMap<String, Long> Pattern_count = new HashMap<String, Long>();
		HashMap<String, Long> node_count = new HashMap<String, Long>();
		List<String> list_PatternBase = new ArrayList<String>();
		logger.info("");
		logger.info("->SUFFIX:" + terminal.toString());
		//PREFIX BUFFERING
		for(Text prefix: prefixs)
		{
			list_PatternBase.add(prefix.toString());
			logger.info("--PREFIX:"+ prefix.toString());
			
			String[] fp_treeNV = prefix.toString().split(",");
			for(String tree_node: fp_treeNV)
			{
				node_name = tree_node.split(":");
				if(node_count.containsKey(node_name[0])== false)
				{
					//각 노드의 이름과 값을 저장함.
					node_count.put(node_name[0], Long.parseLong(node_name[1])); 
					logger.info("NODE:"+ node_name[0] + " COUNT:" + Integer.parseInt(node_name[1]));
				}
				else
				{
					//logger.info("Alert:already exist name " + node_name[0]);
					long nc = node_count.get(node_name[0]);
					if(nc > Long.parseLong(node_name[1]))//신규가 더 작 경우.
					{
						node_count.put(node_name[0], Long.parseLong(node_name[1])); 
					}
				}
			}
		}
		
		//Cond.FP-tree의 각 Path에서 Subset을 만든다.
		logger.info("->SUBSET,MINCOUNT ASSIGN LOOP");
		for(String prefix: list_PatternBase)//INPUT FORM ITEM:COUNT,ITEM:COUNT
		{
			String[] prefix_nodes = prefix.toString().split(","); 
			List<String> List_node = new ArrayList<String>();
			for(String each_node: prefix_nodes)
			{
				node_name = each_node.split(":");
				List_node.add(node_name[0]);
			}
			//Frequent Pattern Count - 부분집합 생성 - 
			/*
			 {2} -> {2}
			 {2,1} -> {2},{2,1}, {2}
			 */
			
			String[] array_node = List_node.toArray(new String[List_node.size()]);			
		
			ICombinatoricsVector<String> initialSet = Factory.createVector(array_node);
			Generator<String> SubSetGen_MinCount = Factory.createSubSetGenerator(initialSet);
			for (ICombinatoricsVector<String> subSet : SubSetGen_MinCount) 
			{	
				if(subSet.getVector().size() == 0) continue;//공집합도 집합
				
				//1ST:최소값 계산함.
				Long min_count = Long.MAX_VALUE;
				logger.info("---SUBSET:" + subSet.getVector().toString());
				for(String item: subSet.getVector())
				{	
					if(min_count > node_count.get(item)) 
					{
						min_count = node_count.get(item);
						logger.info("----node:" + item + ":"+min_count + " (decide)");
					}
				}
				logger.info("---Final MINCOUNT:" + min_count);
				
				//2ST:패턴이 겹칠 경우 누산함.
				String stringVector = subSet.getVector().toString();
				if(Pattern_count.containsKey(stringVector) == false)//Pattern_count 사용 시작 지점 
				//최소값 할당.
				{	
					Pattern_count.put(stringVector, min_count);	
				}
				//동일 노드 누산.`
				else
				{	
					Pattern_count.put(stringVector, min_count + Pattern_count.get(stringVector)); 
				}
			}
		}
		
		logger.info("->SUBSET, RULE GENERATE");
		
		//SUBSET 생성 후 규칙을 만든다.
		for(String prefix: list_PatternBase)
		{
			String[] prefix_nodes = prefix.toString().split(","); 
			List<String> List_node = new ArrayList<String>();

			for(String tree_node: prefix_nodes)
			{
				node_name = tree_node.split(":");	
				List_node.add(node_name[0]);
			}
			
			String[] array_node = List_node.toArray(new String[List_node.size()]);			
			ICombinatoricsVector<String> initialSet = Factory.createVector(array_node);
			Generator<String> SubSetGen_Rule = Factory.createSubSetGenerator(initialSet);
			System.out.println(Arrays.toString(array_node));
			
			//Frequent Pattern Count - 부분집합 생성 - 
			for (ICombinatoricsVector<String> subSet : SubSetGen_Rule) //get 1 subset
			{
				if(subSet.getVector().size() == 0) continue;
				
				logger.info("##SUB.SET:" + subSet.getVector());
				
				String FrequentPatterns = subSet.getVector().toString();				
				subSet.addValue(terminal.toString());//suffix추가.			
				logger.info(subSet.getVector().toString());
				if(pattern_map_rds.containsKey(subSet.getVector()) != true)
				{
					mos.write("patternmaps", subSet.getVector(), Pattern_count.get(FrequentPatterns));
					pattern_map_rds.put(subSet.getVector(), Pattern_count.get(FrequentPatterns));					
				}				
				List<String> ListSubSetVector = subSet.getVector();
				String[] ArrayFrequentPattern = ListSubSetVector.toArray(new String[ListSubSetVector.size()]);				
				//LEFT 1 -> {RIGHT 1,...RIGHT N}
//				logger.info("LEFT 1 -> {RIGHT 1,...RIGHT N}");
				if(ArrayFrequentPattern.length == 2) //LR RULE 최소 길이
				{
					for(int left = 0; left <ArrayFrequentPattern.length; left++)
					{
						for(int right = 0; right< ArrayFrequentPattern.length; right++)
						{
							if(left != right)
							{
								String left_rule = ArrayFrequentPattern[left];
								String right_rule = ArrayFrequentPattern[right];
								
								Rule_configure(left_rule, right_rule);
							}
						}
					}
				}
				else if(ArrayFrequentPattern.length >= 3)
				{
					//PARTITIONER를 사용하여 갯수가 2인 두개의 부분집합을 생성한다.
					logger.info("Get subset from : " + Arrays.toString(ArrayFrequentPattern));
					ICombinatoricsVector<String> vector = Factory.createVector(ArrayFrequentPattern);			
					Generator<ICombinatoricsVector<String>> Rule_Gen = new ComplexCombinationGenerator<String>(vector, 2);	
					logger.info("Success combination");
					try
					{
						logger.info("Rule Gen Iterator create");
						Rule_Gen.Rule_length = rule_length;
						List<ICombinatoricsVector<ICombinatoricsVector<String>>> listRule = Rule_Gen.generateAllObjects();
						
						for(int li = 0 ; li < listRule.size(); li++)
						{
						        List<String> combList = ComplexCombinationGenerator.convert2List(listRule.get(li));
								logger.info("FP LENGTH " +"Idx: " + li + " " + ArrayFrequentPattern.length + "/"+ combList.toString());
								String left_rule = combList.get(0).replace(" ", "");
								left_rule = left_rule.substring(1, left_rule.length()-1);
								
								String right_rule = combList.get(1).replace(" ", "");
								right_rule = right_rule.substring(1, right_rule.length()-1);
								Rule_configure(left_rule, right_rule);
								logger.info("Rule configure success");
						}
					}
					catch(Exception e)
					{
						logger.info(e.toString());
					}
					
				}
				
			}
		}//end of Rule generation		
	}
	/**
	 * LR 규칙 생성 함수.
	 * @author HongJoong.Shin
	 * @date 2015.03.26
	 * @version 0.0.1
	 * @param  String left_rule : 좌측 규칙
	 * @param  String right_rule : 우측 규칙
	 * @return 규칙을 정상적으로 출력하면 1, 오류 발생시 0
	 */
	private int Rule_configure(String left_rule, String right_rule)
	{
		
		try
		{
			if(rule_map_rds.containsKey(left_rule) == true)
			{
				 HashMap<String, Double> right_map_rds = rule_map_rds.get(left_rule);
				 if(right_map_rds.containsKey(right_rule) == false)
				 {
					 //좌측 규칙이 있고 우측 규칙이 없다면 우측 규칙 추가함.
					 if(RuleLength(left_rule) + RuleLength(right_rule) <= rule_length)
					 {
						if(target_item.length() > 0)
						{
							 if((left_rule.indexOf(target_item) >= 0) ||right_rule.indexOf(target_item) >= 0)
							 {
								mos.write("rulemap", left_rule, right_rule);
								right_map_rds.put(right_rule,  0.0);
							 }
						}
						else
						{
							mos.write("rulemap", left_rule, right_rule);
							right_map_rds.put(right_rule,  0.0);
						}
					 }
				 }
			}
			else
			{
				 //좌측 규칙이 없다면 전체 추가함.
				 HashMap<String, Double> right_map_rds = new HashMap<String, Double>();	
				 if((RuleLength(left_rule) + RuleLength(right_rule)) <= rule_length)
				 {
					if(target_item.length() > 0)
					{
						 if((left_rule.indexOf(target_item) >= 0) ||right_rule.indexOf(target_item) >= 0)
						 {
							
							right_map_rds.put(right_rule,  0.0);
							mos.write("rulemap", left_rule, right_rule);						
							rule_map_rds.put(left_rule, right_map_rds);
						 }										 
					}
					else
					{
						mos.write("rulemap", left_rule, right_rule);
						right_map_rds.put(right_rule,  0.0);
						rule_map_rds.put(left_rule, right_map_rds);
					}
				 }
			}
		}
		catch(Exception e)
		{
			logger.info(e.toString());
			return 0;
		}
		return 1;
	}
	/**
	 * 콤마(,)를 규칙 아이템 구분자로 하여 전체 규칙의 길이를 반환.
	 * @author HongJoong.Shin
	 * @date 2015.03.26
	 * @version 0.0.1
	 * @param  String rule : 전체 규칙 
	 * @return 규칙의 길이를 반환
	 */
	private int RuleLength(String rule)
	{
		int len = 0;
		for(int idx = 0; idx < rule.length(); idx++)
		{
			if(rule.charAt(idx) == ',')
			{
				len++;
			}
		}
		return len+1;
	}
	/**
	 * HDFS파일 핸들러 해제.
	 * @author HongJoong.Shin
	 * @date 2015.03.26
	 * @version 0.0.1
	 * @param Context context : 하둡 콘텍스트
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
			mos.close();			
	}
}