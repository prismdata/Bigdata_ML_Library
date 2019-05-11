package org.ankus.mapreduce.algorithms.association.pfpgrowth;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 각 입력 데이터를 특정 구분자로 분리, 분산 캐쉬에 등록된 FList를 기준으로 정렬
 * 정렬된 데이터로 부터 Conditional Pattern Base 출력(key: Suffix, value : Prefix)
 * @author HongJoong.Shin
 * @date 2015.03.26
 * @version 0.0.1
 */
public class PfpgrowthConditionalTransaction_Map extends 
				Mapper<LongWritable , Text, Text, Text>
{
	
	private Logger logger = LoggerFactory.getLogger(PfpgrowthConditionalTransaction_Map.class);
	private String delimiter = "";
	private HashMap<String, Long> flist= new HashMap<String,Long>();	
	
	long minSup=0;
	
	/**
	 * 함수에서 /flist에 저장된 Frequent Item Header Table을 Key(아이템), Value(빈발 수)를 가지는 Hash Type flist로 저장.
	 * @author HongJoong.Shin
	 * @date   2015.03.26
	 * @param  Context context :하둡 콘텍스트 정보 
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
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
        delimiter =  context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
		try
		{
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(int pi = 0; pi < cacheFiles.length; pi++)
			{
				if(cacheFiles != null && cacheFiles.length > 0)
				{
					String line;
					String[] tokens;
					try
					{
						BufferedReader br = new BufferedReader(new FileReader(cacheFiles[pi].toUri().getPath()));
						while((line  = br.readLine())!= null)
						{
							//logger.info(line);
							tokens = line.split("\t");
							if(minSup <= Long.parseLong(tokens[1]))
								flist.put(tokens[0],  Long.parseLong(tokens[1]));
						}
					}
					catch(Exception e)
					{
						logger.info(e.toString());
					}
				}
			}
		}catch(Exception e)
		{
			logger.info(e.toString());
		}
	}
	/**
	 * Input 데이터 파일을 읽고, 구분자로 분리, 각 데이터 마다 flist로 부터 빈발 수를 획득하여 Hash에 저장
	 * Hash에  저장된 데이터를 내림 차순으로 정렬한다. (정렬된 결과는 List형태로 반환)
	 * 각 Input으로 부터 정렬된 데이터에서 마지막 index의 데이터를 suffix,
	 * 그 외의 데이터는 prefix로 하여 출력한다.
	 * 이때 suffix를 두번째 index까지 이동하면서 prefix를 출력한다.
	 * 각 출력 시 Key는 suffix , Value는 Prefix가 된다.
	 * @author HongJoong.Shin
	 * @date 2015.03.26
	 * @param LongWritable key        : 입력 스프릿 오프셋.
	 * @param Text input_transaction  : 입력 스프릿 트렌젝션.
	 * @param Context context         : 하둡 콘텍스트 정보.
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(LongWritable key, Text input_transaction, Context context) throws IOException, InterruptedException
	{		
		Map<String, Long> tmp_input_trans = new HashMap<String,Long>();
 		String tmpSortedTransaction= new String();
 		
 		String items[] = input_transaction.toString().split(delimiter);
		for(String item: items)
		{
			if(flist.containsKey(item) == true)tmp_input_trans.put(item,  flist.get(item));	
		}
		if(tmp_input_trans.size() > 0)
		{
			tmpSortedTransaction  = sortByValue(tmp_input_trans);				
			tmpSortedTransaction = tmpSortedTransaction.substring(0, tmpSortedTransaction.length()-1); //콤마제거 
			Text output_key = new Text();
			Text output_value = new Text();					
			String arrayitems[] = tmpSortedTransaction.split(",");
			int items_size = arrayitems.length-1;			
			//CONFIGURING CONDITIONAL TRANSACTION
			logger.info("Conditional Transation");
			for(int idx = items_size; idx > 0; idx--)//From end To start
			{
				List<String> ListPrefix = new ArrayList<String>();
				String suffix = arrayitems[idx];
				logger.info("#key:[" +suffix+"]");				
				for(int i = 0; i < idx; i++)
				{
					String ai = arrayitems[i];
					ListPrefix.add(ai);
				}				
				output_key.set(suffix);
				String prefix = new String();
				for(String field: ListPrefix)
				{
					prefix += field + ",";
				}
				prefix = prefix.substring(0, prefix.length()-1);
				output_value.set(prefix);
				logger.info("##value:[" +prefix+"]");				
				//OUTPUT CONDITIONAL TRANSACTION
				context.write(output_key,output_value);
			}
		}
	}
	/**
	 * Map 형태의 데이터를 받아 내림 차순으로 정렬한다.
	 * @author HongJoong.Shin
	 * @date 2015.03.26
	 * @param Map map : Map 형태로 저장된 (내림차순) 정렬 대상 인자.
	 * @return String 형태의 (내림차순) 정렬 결과
	 */
	public String sortByValue(final Map map)
	{
        List<String> list = new ArrayList();
        list.addAll(map.keySet());
         
        Collections.sort(list,new Comparator()
        {
            public int compare(Object o1,Object o2)
            {
                Object v1 = map.get(o1);
                Object v2 = map.get(o2);
                return ((Comparable) v1).compareTo(v2);
            }
        });
        Collections.reverse(list); // 주석시 오름차순
        String strList = new String();
        for(int li = 0; li <list.size(); li++)
        {
        	strList += list.get(li) + ",";
        }
        return strList;
    }
}
