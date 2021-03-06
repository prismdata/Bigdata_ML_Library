package org.ankus.mapreduce.algorithms.association.pfpgrowth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Conditional FP Tree를 생성하는 과정으로 PfpgrwothConditionalTransaction_Map로 부터 Suffix와 Prefix를 받는다. 
 * Prefix의 각 value들을 반복적으로 선택하고, HashMap에 저장하면서 발생 수를 누산하여 context에 출력하면 그 결과는 Conditional FP-tree의 형태를 가진다.
 * @author HongJoong.Shin
 * @date 2015.03.26
 * @version 0.0.1
 */
public class PfpgrowthMakeFPTree_Reduce1 extends Reducer <Text,Text, Text, Text >
{
	private Logger logger = LoggerFactory.getLogger(PfpgrowthMakeFPTree_Reduce1.class);
	
	long minSup=0;
	/**
	 * 100분율로 받은 Minimum support로 부터 전체 트랜잭션 중 사용할 support 값으로 변환한다. 
	 * @author HongJoong.Shin
	 * @date 2015.03.26
	 * @param Context context : 하둡 콘텍스트 변수
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		double tc=0;
		double support=0;
		
		Configuration conf = context.getConfiguration();
		tc = Double.parseDouble(conf.get("TRANSACTIONS"));
		support =  context.getConfiguration().getFloat(ArgumentsConstants.AR_MINSUPP,1) * 100;
		if(support >= 100)
		{
			support = 1;
		}
        minSup =  (long)((support / 100) * tc);
        
        logger.info("Min support: " + minSup + "(records)");
	}
	/**
	 * PfpgrwothConditionalTransaction_Map로 부터 Suffix와 Prefix를 받는다. 
	 * prefix의 1번째가 HashMap에 존재하는 경우 해당 prefix를 HashMap Key의 Value에 추가하고,
	 * 없는 경우 1번째 데이터를 HashMap Key로 추가하고 prefix를 Value에 추가한다
	 * prefix의 각 value들을 반복적으로 선택하고, HashMap에 저장하면서 발생 수를 누산하여 context에 출력한다.
	 * @author HongJoong.Shin
	 * @date 2015.03.26
	 * @param Text suffix : Mapper의 Key인 Suffix
	 * @param Iterable<Text>  set_prefix  : Mapper의 Output인 prefix 집합
	 * @param Context context  : 하둡 콘텍스트 변수
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(Text suffix, Iterable<Text> set_prefix, Context context)
			throws IOException, InterruptedException
	{
			logger.info("Conditional Databse");
			logger.info("#Key:[" + suffix.toString() + "]");
			//Conditional FP-TREES 생성
			//트리의 BRANCH를 자료구조로 표현하기 HashMap을 사용한다.
			//HashMap의 키는 Text Type 0TH ITEM이고 Value는 Text Type List이다.
			//prefix의 0번째 ITEM이 HashMap에 있으면 List에 추가하고(하나의 트리 생성)
			//없으면 0번째 ITEM을 MashMap에 추가하면서 List도 추가한다.(BRANCH TREE 생성)
			HashMap<String, List<Text>> FP_Tree = new HashMap<String,List<Text>>();
			for(Text new_prefix : set_prefix)
			{			
				Text prefix = new Text();
				prefix.set(new_prefix.toString());
				logger.info("prefix:[" +prefix.toString()+"]");
				//head의 아이템 추출 방법 변경
				String HEAD = prefix.toString().split(",")[0];
				
				//동일한 BRANCH인 경우 
				if(FP_Tree.containsKey(HEAD)==true)
				{
					List<Text> BODY = FP_Tree.get(HEAD);
					BODY.add(prefix);
					FP_Tree.put(HEAD, BODY);
				}
				else//신규 BRANCH인 경우.
				{
					List<Text> BODY = new ArrayList<Text>();
					BODY.add(prefix);
					FP_Tree.put(HEAD,BODY);
				}				
			}			
			Text outValue_CondFP_TREE = new Text();
			//HASH MAP MEMORY DUMP			
			Set<String> SubRoots = FP_Tree.keySet();			 
			logger.info("SubRoots:" + SubRoots.toString());
			//HEAD(subroot)까지는 트리 수만큼 나와야 한다.
			for(String SubRoot : SubRoots)
			{
					Map<String, Integer> body_count = new HashMap<String, Integer>();
					logger.info("SubRoot:" + SubRoot);
					List<Text> List_Childs = FP_Tree.get(SubRoot);
					//Node occurrences count
					for(Text child:List_Childs)
					{
						//CHILD 노드들의 갯수를 누산함.
						String[] nodes = child.toString().split(",");
						for(String node: nodes)
						{
							node = node.replace(" ", "");
							if(body_count.containsKey(node)==true)
							{
								body_count.put(node, body_count.get(node)+1);
								logger.info("Update node:" + node + " count:" + body_count.get(node));
							}
							else
							{
								body_count.put(node, 1);
								logger.info("Insert node:" + node + " count:" + body_count.get(node));
							}
						}
					}
					//Output Formmatting
					Set<String> childs = body_count.keySet();
					String child_path =new String();
					int numChild = 0;
					for(String child: childs)
					{
						if(minSup <= body_count.get(child))
						{
							child_path += child + ":" + body_count.get(child) + ",";
							numChild ++;
						}
					}
					if(numChild > 0)
					{
						child_path = child_path.substring(0, child_path.length()-1);
						logger.info(child_path);
						outValue_CondFP_TREE.set(child_path);			
						context.write(suffix,outValue_CondFP_TREE);	
						logger.info("");
					}
			}//end of SubRoots loop
	}
}