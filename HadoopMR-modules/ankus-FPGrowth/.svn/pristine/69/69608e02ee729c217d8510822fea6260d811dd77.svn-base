package org.ankus.mapreduce.algorithms.association.pfpgrowth;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.ankus.util.Usage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
//import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PFP_growth Driver
 * @author HongJoong.Shin
 * @date 2015.03.26
 * @version 0.0.1
 */
public class PfpgrowthDriver extends Configured implements Tool{
	
	HashMap<String, HashMap<String, Double>> rule_map_Drv = new HashMap<String, HashMap<String, Double>>();
	HashMap<Set<String>,  Long > pattern_map_Drv = new HashMap<Set<String>, Long>();
	
	int rule_length = 0;
	Configuration conf = null;
	double top_rate = 0.0;
	
	int metric_case = 0;
	double min_confidence = 0.0;
	double min_lift = 0.0;
	double tc =0.0;
	String target_item="";
	String metric_type ="";
	double support=0;
	
	String resultPath_1 = "/flist";
	String resultPath_2 = "/FPtree";
	String resultPath_3 = "/rule";
	FileSystem fs = null;
	HashMap<String, Double> Flist_map = new HashMap<String, Double>();
	
	private Logger logger = LoggerFactory.getLogger(PfpgrowthDriver.class);
	
	String str_flist = new String();
	Path path;
	FileStatus[] status;
	/**
     * main()함수로 ToolRunner를 사용하여 FPGrwoth 기능을 호출한다.
     * @author  HongJoong.Shin
     * @param String[] args : FPGrwoth 알고리즘 수행 인자
     * @author HongJoong.Shin
     * @date   2015.03.26
     * @version 0.0.1
     * @return 없음.
     */
	public static void main(String[] args) throws Exception
	{	
		int res = ToolRunner.run( new PfpgrowthDriver(), args);
		System.exit(res);
	}
	
	/**
     * ToolRunner에서 호출되는 실제 알고리즘 시작 함수.
     * @author HongJoong.Shin
     * @date 2015.03.26
     * @version 0.0.1
     * @param String[] args : FPGrwoth 알고리즘 수행 인자
     * @return int : 처리 결과 상태 값으로 오류 발생시 1, 정상 종료시 0을 리턴
     */
	public int run(String[] args) throws Exception
	{
		long endTime = 0;
	   	long lTime  = 0;
	   	long startTime = 0 ;		
		
		conf = getConf();
		//-minSup 2 -delimiter \t -maxRuleLength 3 -ruleCount 100 -matricType confidence -metricValue 20 -targetItemList 2
		if(!ConfigurationVariable.setFromArguments(args, conf))
		{
			logger.error("MR Job Setting Failed..");
            Usage.printUsage(Constants.DRIVER_PFPGROWTH_ASSOCIATION); 
            logger.info("Error: MR Job Setting Failed..: Configuration Error");
            return 1;
		}		
		startTime = System.nanoTime();
		rule_length = conf.getInt(ArgumentsConstants.AR_MAX_RULE_LENGTH, Integer.MAX_VALUE);
		logger.info("Debug Rule Length: " + rule_length);
		Job job1 = new Job(this.getConf());
		FileInputFormat.addInputPaths(job1, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job1, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + resultPath_1));
		
		//make frequent list with support count
		job1.setJarByClass(PfpgrowthDriver.class);
		job1.setMapperClass(PfpgrowthSupportCountMapper.class);			
		job1.setReducerClass(PfpgrowthSupportCountReducer.class);		
		job1.setCombinerClass(PfpgrowthSupportCounterCombiner.class);		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);		
		if(!job1.waitForCompletion(true))
    	{
        	logger.error("Error: MR for Item Count is not Completeion");
            logger.info("MR-Job for Item Count is Failed..");
        	return 1;
        }
		long tc = job1.getCounters().findCounter("Pfpgrowth", "TRANSACTIONS").getValue();
		logger.info("NUM TRANSACTIONS:" + tc);				
		
		conf.set("TRANSACTIONS", Long.toString(tc));
		Job job2 = new Job(this.getConf());		
		
		//분산 케쉬에 헤더 데이블 정보 저장.(로컬 ACCESS확인 필요).		
		fs = FileSystem.get(conf);
		path = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + resultPath_1);
		status = fs.listStatus(path);
		for (int i=0;i<status.length;i++){
			Path ipath = status[i].getPath();
			logger.info("FList path : " + ipath.getName());
			if(ipath.getName().indexOf("part-r") >= 0)
			{
				logger.info("CacheFile:"+ conf.get(ArgumentsConstants.OUTPUT_PATH) + resultPath_1 + "/"+ipath.getName());
				String strpath = conf.get(ArgumentsConstants.OUTPUT_PATH) + resultPath_1 + "/"+ipath.getName();
				DistributedCache.addCacheFile(new Path(strpath).toUri(), job2.getConfiguration());
			}
		}
		//Frequent Pattern Tree 출력
		FileInputFormat.addInputPaths(job2, conf.get(ArgumentsConstants.INPUT_PATH));
        FileOutputFormat.setOutputPath(job2, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + resultPath_2));
		//make Conditional FP-TREE
		job2.setJarByClass(PfpgrowthDriver.class);	    
		job2.setMapperClass(PfpgrowthConditionalTransaction_Map.class);
		job2.setReducerClass(PfpgrowthMakeFPTree_Reduce1.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);		
		if(!job2.waitForCompletion(true))
    	{
        	logger.error("Error: MR for Conditional FT-TREE is not Completeion");
            logger.info("MR-Job for Conditional FT-TREE is Failed..");
        	return 1;
        }
		logger.info("FP-TREE FINISH");
		
		//FP-TREE에서 RULE생성.
		conf.set("TRANSACTIONS", tc+"");		
		Job job3 = new Job(this.getConf());	
		set3StepJob(job3, conf, resultPath_2, resultPath_3);
		
		//make running fp-growth
		job3.setJarByClass(PfpgrowthDriver.class);	    
		job3.setMapperClass(PfpgrowthPatternGen_Map.class);
		job3.setReducerClass(PfpgrowthPatternGen_ReduceFP_List2.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job3, "rulemap", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job3, "patternmaps", TextOutputFormat.class, Text.class, Text.class);
		
		if(!job3.waitForCompletion(true))
    	{
        	logger.error("Error: MR for Conditional FT-TREE is not Completeion");
            logger.info("MR-Job for Conditional FT-TREE is Failed..");
        	return 1;
        }
		
		load_input();
		logger.info("Get Rule confidence and lift");
		rule_measure(job3.getConfiguration());
		logger.info("GEN RULE FINISH");
		
		String output_root = conf.get(ArgumentsConstants.OUTPUT_PATH);
		// temp delete process
        if(conf.get(ArgumentsConstants.TEMP_DELETE, "true").equals("true"))
        {
            FileSystem.get(conf).delete(new Path(output_root + resultPath_1), true);
            logger.info("DELETE " + output_root + resultPath_1);
            FileSystem.get(conf).delete(new Path(output_root + resultPath_2), true);
            logger.info("DELETE " + output_root + resultPath_2);
            FileSystem.get(conf).delete(new Path(output_root + resultPath_3), true);
            logger.info("DELETE " + output_root + resultPath_3);
            logger.info("> Temporary Files are Deleted..");
        }
        endTime = System.nanoTime();
		lTime = endTime - startTime;
		
		System.out.println("PFP Growth Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
		return 0;		
	}
	
	/**
	 * FP-TREE에서 RULE생성을 위한 job의 입출력 경로를 설정.
	 * @author HongJoong.Shin
     * @date 2015.03.26
     * @version 0.0.1
	 * @param Job job : Job에 대한 설정 인자
	 * @param Configuration conf : 하둡 실행 설정 인자
	 * @param String inputPathStr : 입력 경로
	 * @param String outputPathStr : 출력 경로
	 * @throws IOException : 출력 경로가 비어 있지 않을 경우 예외 발생
	 */
	private void set3StepJob(Job job, Configuration conf, String inputPathStr , String outputPathStr) throws IOException
    {
        // TODO
		String inputPath = conf.get(ArgumentsConstants.OUTPUT_PATH)+ inputPathStr;
		
        FileInputFormat.addInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + outputPathStr));
    }
	/**
	 * FREQUENT MAP과 GENERATED RULE을 HDFS에서 읽어 HashMap에 저장함.
	 * @author HongJoong.Shin
     * @date 2015.03.26
     * @version 0.0.1
	 * @return 정상으로 로딩된 경우 0, 오류 발생시 -1리턴.
	 */
	private int load_input()
	{
		try
		{
			fs = FileSystem.get(conf);		
			path = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + resultPath_1);
			status = fs.listStatus(path);
			for (int i=0;i<status.length;i++)
			{
				Path ipath = status[i].getPath();
				//logger.info("FList path : " + ipath.getName());
				if(ipath.getName().indexOf("part-r") >= 0)
				{
					if (read_flist_str(fs, ipath) == -1)
					{
						return -1;
					}
				}
	        }
			path = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + resultPath_3);
			status = fs.listStatus(path);
			for (int i=0;i<status.length;i++)
			{
				Path ipath = status[i].getPath();
				//FREQUENT PATTERN LOAD
				if(ipath.getName().indexOf("patternmaps-r") >= 0)
				{
					String line = new String();
					BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(ipath)));
			        
					while(line != null)
					{
						line=br.readLine();
						if(line == null)
						{
							break;
						}
				        String token[] = line.split("\t");
				        //token[0] = "PATTERN
				        //token[1] = "COUNT"			        
				        token[0] = token[0].substring(1, token[0].length()-1);
				        String items[] = token[0].split(",");
				        
				        Set<String> ptrnSet = new HashSet<String>();
				        //2번째 아이템 부터는 공백 제거
				        for(int idx = 0; idx< items.length; idx++)
				        {
				        	if(idx >=1)
				        	{
				        		items[idx] = items[idx].substring(1, items[idx].length());
				        	}
				        	ptrnSet.add(items[idx]);
				        }
				        pattern_map_Drv.put(ptrnSet, Long.parseLong(token[1]));
					}
				}
			}
			for (int i=0;i<status.length;i++)
			{
				//RULE MAP LOAD
				Path ipath = status[i].getPath();
				if(ipath.getName().indexOf("rulemap-r") >= 0)
				{
					String line = new String();
					BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(ipath)));
			        
					while(line != null)
					{
						line=br.readLine();
						if(line == null)
						{
							break;
						}
				        String token[] = line.split("\t");
				        //token[0] = "left rule"
				        //token[1] = "right rule"
				        HashMap<String, Double> right_rule = new HashMap<String, Double>();
				        right_rule.put(token[1], 0.0);
				        
				        if(rule_map_Drv.containsKey(token[0]) == true)
				        {
				        	HashMap<String , Double> referenceValue = rule_map_Drv.get(token[0]);
				        	if(referenceValue.containsKey(token[1]) == false)
				        	{
				        		referenceValue.put(token[1], 0.0);
				        	}
				        }
				        else
				        {
				        	rule_map_Drv.put(token[0],  right_rule);
				        }
					}
				}
			}
		}
		catch(Exception e)
		{
			logger.info(e.toString());
			return -1;
		}
		return 0;
	}
	/**
	 * 연관 규칙 평가 척도인 confidence, left와 최대 규칙 길이를 적용하여 생성된 규칙을 파일에 저장함.
	 * @author HongJoong.Shin
     * @date 2015.03.26
     * @version 0.0.1
	 * @param Configuration conf : 하둡 환경 변수
	 * @return 정상 수행시 0, 오류 발생시 -1을 리턴함
	 */
	private int  rule_measure(Configuration conf )
	{
		int rule_length = 0;
		double top_rate = 0.0;
		
		int metric_case = 0;
		double min_confidence = 0.0;
		double min_lift = 0.0;
		double tc =0.0;
		String metric_type ="";
		double support=0;
		long minSup=0;
		try
		{			
			target_item = conf.get(ArgumentsConstants.AR_TARGET_ITEM, "");			
			//최대 규칙 길이 제어 
			rule_length = conf.getInt(ArgumentsConstants.AR_MAX_RULE_LENGTH, Integer.MAX_VALUE);
			if(rule_length < 0)
			{
				rule_length = Integer.MAX_VALUE;
			}
			//최대 규칙 갯수 제어
			//top_rate = Double.parseDouble(conf.get(ArgumentsConstants.AR_RULE_COUNT ,"100")); //%
			top_rate = Double.parseDouble(conf.get(ArgumentsConstants.AR_RULE_COUNT ,Integer.MAX_VALUE +""));
			if(top_rate <= 0)
			{
				top_rate = Integer.MAX_VALUE;
			}
			
			metric_type = conf.get(ArgumentsConstants.AR_METRIC_TYPE, "confidence");
			if(metric_type.toLowerCase().equals("confidence") == true)
			{
				metric_case = 0; //metric = confidence
				min_confidence = Double.parseDouble(conf.get(ArgumentsConstants.AR_METRIC_VALUE,"20"));
				//confidence 최소값 제어 
				if(min_confidence <= 0)
				{
					min_confidence = 1;
				}
			}
			else
			{
				metric_case = 1; //metric = lift
				min_lift = Double.parseDouble(conf.get(ArgumentsConstants.AR_METRIC_VALUE,Double.MIN_VALUE+""));
				//confidence 최소값 제어 
				if(min_lift <= 0)
				{
					min_lift = Double.MIN_VALUE;
				}			
			}	
			tc = Double.parseDouble(conf.get("TRANSACTIONS"));
			support =  conf.getFloat(ArgumentsConstants.AR_MINSUPP,1) * 100;
			if(support >= 100)
			{
				support = 1;
			}
	        minSup =  (long)((support / 100) * tc);
	        logger.info("Min support: " + minSup + "(records)");
		}
		catch(Exception e)
		{
			logger.error(e.toString());
		}
		Map<String , Double> rule_confidence_map = new HashMap<String, Double>();
		Map<String , Double> rule_lift_map = new HashMap<String, Double>();
		
		Set<String> leftRuleSet = rule_map_Drv.keySet();
		for(String leftRule : leftRuleSet)
		{
			HashMap<String, Double > right_map = rule_map_Drv.get(leftRule);
			Set<String> rightRuleSet = right_map.keySet();
						
			for(String rightRule: rightRuleSet)
			{		
				double confi_denominator = 0.0;
				double confi_numerator = 0.0;
				double confidence = 0.0;		
				
				Set<String> cmp_rulefull_set  = new HashSet<String>();
				Set<String> SetStrLeftRule = new HashSet<String>(Arrays.asList(leftRule.split(",")));
				Set<String> SetStrRightRule = new HashSet<String>(Arrays.asList(rightRule.split(",")));
			
				cmp_rulefull_set.addAll(SetStrLeftRule);
				cmp_rulefull_set.addAll(SetStrRightRule);				
				if(SetStrLeftRule.size() == 1)
				{
					confi_denominator = Flist_map.get(leftRule); 	
					confi_numerator =  pattern_map_Drv.get(cmp_rulefull_set);
					confidence = ( confi_numerator / confi_denominator)*100;																		
					rule_confidence_map.put(leftRule + "->"+rightRule, confidence);
				}
				else
				{
					confi_denominator = pattern_map_Drv.get(SetStrLeftRule);
					confi_numerator =  pattern_map_Drv.get(cmp_rulefull_set);
					confidence = (confi_numerator/confi_denominator)*100;									
					rule_confidence_map.put(leftRule + "->"+rightRule, confidence);
				}
			}
		}		
		if(tc == 0)
		{
			logger.info("LIFT FAIL");
			return -1;
		}
		
		for(String leftRule : leftRuleSet)
		{
			HashMap<String, Double > right_map = rule_map_Drv.get(leftRule);
			Set<String> rightRuleSet = right_map.keySet();
		
			for(String rightRule: rightRuleSet)
			{		
					double lift_denominator = 0.0; //분모 
					double lift_numerator = 0.0; //분자 
					double lift_left = 0.0, lift_right = 0.0;
					double lift = 0.0;

					Set<String> cmp_rulefull_set  = new HashSet<String>();
					Set<String> SetStrLeftRule = new HashSet<String>(Arrays.asList(leftRule.split(",")));
					Set<String> SetStrRightRule = new HashSet<String>(Arrays.asList(rightRule.split(",")));
								
					cmp_rulefull_set.addAll(SetStrLeftRule);
					cmp_rulefull_set.addAll(SetStrRightRule);
					
					if(SetStrLeftRule.size() == 1 )
					{
						//LEFT 규칙 길이가 1인 경우 Flist참조.
						lift_left = Flist_map.get(leftRule); //FLIST에서 COUNT 획득 A규칙 갯수로 사용
						
						//RIGHT 규칙의 COUNT 얻기.
						if(Flist_map.containsKey(rightRule)==true)//FLIST에서 COUNT 획득 B규칙 갯수로 사용
						{
							lift_right = Flist_map.get(rightRule);
						}
						else
						{
							if(pattern_map_Drv.containsKey(SetStrRightRule) == true)
							{
								lift_right = pattern_map_Drv.get(SetStrRightRule);
							}						
						}
						
						//분자의 COUNT 계산
						if(pattern_map_Drv.containsKey(cmp_rulefull_set) == true)
						{
							lift_numerator =  pattern_map_Drv.get(cmp_rulefull_set);
						}
						
						lift_denominator = lift_right +lift_left;						
						lift = ((lift_numerator*tc) / lift_denominator);
						String slift =lift+"";
						
						rule_lift_map.put(leftRule + "->"+rightRule, lift);
					}
					else 
					{
						//LEFT 규칙 길이가 1이상인 경우.
						if(pattern_map_Drv.containsKey(SetStrLeftRule) == true)
						{
							lift_left =  pattern_map_Drv.get(SetStrLeftRule);
						}
						if(pattern_map_Drv.containsKey(SetStrRightRule) == true)
						{
							lift_right =  pattern_map_Drv.get(SetStrRightRule);
						}
						if(pattern_map_Drv.containsKey(cmp_rulefull_set) == true)
						{
							lift_numerator =  pattern_map_Drv.get(cmp_rulefull_set);
						}
						
						//분모 
						lift_denominator = lift_right +lift_left;
						lift = ((lift_numerator*tc) / lift_denominator);///100;
						rule_lift_map.put(leftRule + "->"+rightRule, lift);
					}
				}
		}
		
		int rc = 1;
		Map<String, Double> rule_metric_map = null;
		switch(metric_case)
		{
			case 0:
				rule_metric_map = rule_confidence_map;
				logger.info("Confidence");
				break;
			case 1:
				rule_metric_map = rule_lift_map;
				logger.info("Lift");
				break;
		}
		
		@SuppressWarnings("rawtypes")
		Iterator it = sortByValue(rule_metric_map).iterator();
		double total_rule_cnt = (double)rule_metric_map.size();
		
		//int top_N =(int) (total_rule_cnt * (top_rate/ 100));
		int top_N =(int) (top_rate);
		
		try
		{
			Path hdfs_path = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/rule_result.txt");
			FSDataOutputStream fs_outstream = fs.create(hdfs_path);
			PrintWriter writer  = new PrintWriter(fs_outstream);
	        while(it.hasNext())
	        {
	            String rule = (String) it.next();
	            boolean metric_out_condition =false;
	            switch(metric_case)
	            {
	            case 0:
	            	if(min_confidence > rule_metric_map.get(rule))
	            	{
	            		metric_out_condition = true;
	            	}
	            	break;
	            case 1:
	            	if(min_lift > rule_metric_map.get(rule))
	            	{
	            		metric_out_condition = true;
	            	}
	            	break;
	            }
	            if(metric_out_condition == true) break;
	            
	            String delimiter =  conf.get(ArgumentsConstants.DELIMITER, "\t");
	            double metric_value = rule_metric_map.get(rule)/100;
	            
	            rule = rule.replace(",", delimiter);
	            rule = rule.replace("->", "@@");
	            
	            logger.info("("+rc +") "+ rule + "@@" + " " + metric_type +" "+ metric_value);
	            writer.write(rc + "@@" + rule + "@@" + metric_type +"@@"+ metric_value  +"\n");
	            
	            if(rc >= top_N)
	            {
	            	break;
	            }
				rc++;
	        }
	        writer.close();
	        fs_outstream.close();
		}
		catch(Exception e)
		{
			return -1;
		}
		return 0;
			
	}
	/**
	 * hdfs_flist_path로 부터 아이템과 발생 횟수를 HashMap으로 로딩한다.
	 * @author HongJoong.Shin
     * @date 2015.03.26
     * @version 0.0.1
	 * @param FileSystem hdfs : 하둡 파일 시스템 객체
	 * @param Path shdfs_flist_path : 아이템 발생 횟수가 기록된 경로
	 * @return
	 */
	public int read_flist_str(FileSystem hdfs, Path hdfs_flist_path) 
	{	
		try
		{
			BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(hdfs_flist_path)));    
			String line=br.readLine();
	        String token[] = line.split("\t");
	        Flist_map.put(token[0], Double.parseDouble(token[1]));
	        
	        while (true)
	        {
	                line=br.readLine();
	                if (line == null) break;
	                String token2[] = line.split("\t");	                
	                Flist_map.put(token2[0], Double.parseDouble(token2[1]));
	        }
	        
			return 0;		
		}
		catch(Exception e)
		{
			return -1;
		}
	}
	/**
	 * Map의 내용을 Value를 기준으로 내림 차순 정렬함.
	 * @author HongJoong.Shin
     * @date 2015.03.26
     * @version 0.0.1
	 * @param final Map map : 아이템과 발생 횟수 Map형태로 저장된 인자
	 * @return 정렬된 아이템 리스트
	 */
	public List sortByValue(final Map map)
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
        return list;
    }
}
