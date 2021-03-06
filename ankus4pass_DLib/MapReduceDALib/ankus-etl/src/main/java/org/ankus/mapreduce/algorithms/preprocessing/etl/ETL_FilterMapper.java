/*
 * Copyright (C) 2011 ankus (http://www.openankus.org).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.ankus.mapreduce.algorithms.preprocessing.etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

/**
 * ColumnExtractor,FilterInclude,Replace,FilterExclude,NumericNorm 수행을 위한 데이터 추출,변환, 작업을 수행함.
 * @author HongJoong.Shin
 * @date   2016.12.06
 */
public class ETL_FilterMapper extends Mapper<Object, Text, NullWritable, Text>
{
	private Logger logger = LoggerFactory.getLogger(ETL_FilterMapper.class);
	private String mDelimiter;	
	private String filter_method ="";	
    private HashMap<String, String> hash_EvalNumericNorm 
    								= new HashMap<String, String>();
    
	private HashMap<Double, HashMap<Double, String>> hash_NumericNormRule
									= new HashMap<Double , HashMap<Double, String>>();	
	
	private HashMap<Integer, List<String>> hash_columnIncludeExcludeRule = new HashMap<Integer, List<String>>();	
	
	private HashMap<Integer, HashMap<String, String>> hash_columnReplaceRule = new HashMap<Integer, HashMap<String, String>>();
	
	String replaced_key = "";
	String filter_target = "";
	String filter_target_cp = null;
	String extrudded_value = "";
	String filter_rule_path = null;
	String filter_replace_rule = "";
	
	private int indexArray[]; //연산할 컬럼 번호 행렬
	private int exceptionIndexArr[];//연산에서 제외할 컬럼 번호 행렬.
	
	/**
	 * ETL 기본 기능을 열거형으로 정의.
	 * @author HongJoong.Shin
	 * @date   2016.12.06
	 */
	private enum ETL_T_Method {
		Replace, ColumnExtractor, FilterInclude, FilterExclude, Sorting, NumericNorm;
	}
	/**
	 * 실수형으로 변환 가능한 문자열인지 검사함.
	 * @author HongJoong.Shin
	 * @date   2016.12.06
	 * @parameter String s : 검사할 문자열
	 * @return boolean true: 변환 가능, false : 변환 불가.
	 */
    private static boolean isStringDouble(String s)
    {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }  
    }
    
    /**
     * 파일 혹은 실행 인자로 부터 얻은 문자열 규칙을 파싱한다.
     * @author HongJoong.Shin
	 * @date   2016.12.06
	 * @param  Context context 하둡 환경 설정 변수
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    	try
	    {
	        mDelimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");	        
	        filter_method = context.getConfiguration().get(ArgumentsConstants.ETL_T_METHOD, "");//required	        
	        
	        Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);			
			String value = filter_method;
			ETL_T_Method method = ETL_T_Method.valueOf(value); // surround with try/catch
			switch(method) 
			{
				/**
				 *ETL 기능이 특정 컬럼 추출일 경우.
				 *@author HongJoong.Shin
				 *@date    2016.12.06
				 */
				case ColumnExtractor:
					//추출할 컬럼과 추출에서 제외할 컬럼을 획득함.
					indexArray = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX,  "-1"));
			        exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
					break;
					
				
				/**
				 *ETL 기능이 수치를 카테고리로 변환하는 경우.
				 *@author HongJoong.Shin
				 *@date   2016.12.06
				 */
			  	case NumericNorm:
			  		try
			  		{
			  			indexArray = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.TARGET_INDEX,  "-1"));
				        exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(context.getConfiguration().get(ArgumentsConstants.EXCEPTION_INDEX, "-1"));
				        //정규화 규칙 파일 경로 획득 
			  			String number_norm_rule_path = context.getConfiguration().get(ArgumentsConstants.ETL_NUMERIC_NORM_RULE_PATH
			  																											,null);
			  			//규칙 파일 경로가 설정되어 있는 경우 
				        if(number_norm_rule_path != null)
				        {
				        	Path path = new Path(number_norm_rule_path);
							BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
							String line = "";
							//Start scan
							while ((line = in.readLine())!= null)
							{
								//1개 이상의 규칙이 @@로 구분되어 있을 경우를 고려하여 문자열 규칙을 배열로 전환하고, 
								//각 규칙을 HashMap에 저장함.(Key: 사전 조건, Value: 교채할 값)
								String[] NormRuleToken = line.split("@@"); //10~50->LITTLE&&51~80->NORMAL&&81~->HEAVY
					  			//logger.info(line);		  			
					  			for(int nrt  = 0; nrt < NormRuleToken.length; nrt++)
					  			{
					  				String rule_pattern = NormRuleToken[nrt]; //X1~X2->Y1
					  				String[] rule_token = rule_pattern.split("->"); //X1~X2
					  				hash_EvalNumericNorm.put(rule_token[0], rule_token[1]);
					  			}
					  			logger.info("NumericNorm load finish");	
							}
				        }
			  			
				  		String StrNurmericform =  context.getConfiguration().get(ArgumentsConstants.ETL_NUMERIC_NORM, null);
				  		if(StrNurmericform != null)
				  		{
				  			if(StrNurmericform.substring(0, 1).equals("'"))
							{
								StringBuffer sb = new StringBuffer( StrNurmericform );
								sb.setCharAt(0, ' '); 
								StrNurmericform = sb.toString();
							}
							//0th character replace " '
							if(StrNurmericform.substring(0, 1).equals("\""))
							{
								StringBuffer sb = new StringBuffer( StrNurmericform );
								sb.setCharAt(0, ' ');
								StrNurmericform = sb.toString();
							}
							//last th character replace " '
							if(StrNurmericform.substring(StrNurmericform.length()-1, StrNurmericform.length()).equals("'"))
							{
								StringBuffer sb = new StringBuffer( StrNurmericform );
								sb.setCharAt(StrNurmericform.length()-1, ' ');
								StrNurmericform = sb.toString();
							}
							//last th character replace ' "
							if(StrNurmericform.substring(StrNurmericform.length()-1, StrNurmericform.length()).equals("\""))
							{
								StringBuffer sb = new StringBuffer( StrNurmericform );
								sb.setCharAt(StrNurmericform.length()-1, ' ');
								StrNurmericform = sb.toString();
							}
							StrNurmericform = StrNurmericform.trim();
							
				  			String[] NormRuleToken = StrNurmericform.split("@@"); //10~50->LITTLE@@51~80->NORMAL@@81~->HEAVY
				  			//logger.info(StrNurmericform);		  			
				  			for(int nrt  = 0; nrt < NormRuleToken.length; nrt++)
				  			{				  				
				  				String rule_pattern = NormRuleToken[nrt]; //X1~X2->Y1				  				
				  				String[] rule_token = rule_pattern.split("->"); //범위와 규칙으로 나눈다.				  				
				  				hash_EvalNumericNorm.put(rule_token[0], rule_token[1]);
				  			}

				  		}
			  		}
			  		catch(Exception e)
			  		{
			  			logger.info(e.toString());
			  		}
			  		
				  break;
				  
			  /**
			   * 특정 문자열을 대상 문자열로 변경한다.
			   * @author HongJoong.Shin
			   * @date    2016.12.06
			   */
			    case Replace:
			    	//문자열 변경 규칙 경로를 가져온다.
			        String filter_replace_rule_path = context.getConfiguration().get(ArgumentsConstants.ETL_REPLACE_RULE_PATH, null);
			        if(filter_replace_rule_path != null) //파일 기반.
			        {
			        	Path path = new Path(filter_replace_rule_path);
						BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
						String line = "";
						//Start scan
						while ((line = in.readLine())!= null)
						{
							String[] rules = line.split("@@");
							for(int ri = 0; ri < rules.length; ri++)
							{
								String[] token = rules[ri].split(",");
								int column_id = Integer.parseInt(token[0]);
								
								if(hash_columnReplaceRule.containsKey(column_id) == false)
								{
									HashMap<String, String> hash_rule = new HashMap<String, String>();
									hash_rule.put(token[1], token[2]);
									hash_columnReplaceRule.put(column_id, hash_rule);							
								}
								else
								{
									HashMap<String, String> hash_rule = hash_columnReplaceRule.get(column_id);
									hash_rule.put(token[1], token[2]);
								}
							}
						}
			        }
			        else //명령창 기반.
					{
			        	//파리미터로 들어온 규칙을 파싱하여 hash_columnReplaceRule에 저장.
			        	filter_replace_rule = context.getConfiguration().get(ArgumentsConstants.ETL_REPLACE_RULE, null);
			        	if(filter_replace_rule.substring(0, 1).equals("'"))
						{
							StringBuffer sb = new StringBuffer( filter_replace_rule );
							sb.setCharAt(0, ' '); 
							filter_replace_rule = sb.toString();
						}
						//0th character replace " '
						if(filter_replace_rule.substring(0, 1).equals("\""))
						{
							StringBuffer sb = new StringBuffer( filter_replace_rule );
							sb.setCharAt(0, ' ');
							filter_replace_rule = sb.toString();
						}
						//last th character replace " '
						if(filter_replace_rule.substring(filter_replace_rule.length()-1, filter_replace_rule.length()).equals("'"))
						{
							StringBuffer sb = new StringBuffer( filter_replace_rule );
							sb.setCharAt(filter_target.length()-1, ' ');
							filter_replace_rule = sb.toString();
						}
						//last th character replace ' "
						if(filter_replace_rule.substring(filter_replace_rule.length()-1, filter_replace_rule.length()).equals("\""))
						{
							StringBuffer sb = new StringBuffer( filter_replace_rule );
							sb.setCharAt(filter_replace_rule.length()-1, ' ');
							filter_replace_rule = sb.toString();
						}
						filter_replace_rule = filter_replace_rule.trim();
						
						String[] target_cols = filter_replace_rule.split("@@");
						for(int ti = 0; ti < target_cols.length; ti++)
						{
							String[] token = target_cols[ti].split(",");
							int column_id = Integer.parseInt(token[0]);
							if(hash_columnReplaceRule.containsKey(column_id) == false)
							{
								HashMap<String, String> hash_rule = new HashMap<String, String>();
								hash_rule.put(token[1], token[2]);
								hash_columnReplaceRule.put(column_id, hash_rule);							
							}
							else
							{
								HashMap<String, String> hash_rule = hash_columnReplaceRule.get(column_id);			
								hash_rule.put(token[1], token[2]);
							}
						}
					}
			        break;	
		        /**
		         * 특정 컬럼의 특정 값을 갖는 레코드를 제거한다.
		         * @author HongJoong.Shin
		         * @date    2016.12.06
		         */
			    case FilterExclude:
		    	/**
		         * 특정 컬럼의 특정 값을 갖는 레코드만 추출한다.
		         * @author HongJoong.Shin
		         * @date    2016.12.06
		         */
			    case FilterInclude:			    	
			    	 filter_rule_path = context.getConfiguration().get(ArgumentsConstants.ETL_RULE_PATH, null);
					if(filter_rule_path == null)//FILE을 사용하지 않고 규칙을 적용하는 경우
					{
						filter_target = context.getConfiguration().get(ArgumentsConstants.ETL_RULE, null);
						//0th character replace ' "
						if(filter_target.substring(0, 1).equals("'"))
						{
							StringBuffer sb = new StringBuffer( filter_target );
							sb.setCharAt(0, ' '); 
							filter_target = sb.toString();
						}
						//0th character replace " '
						if(filter_target.substring(0, 1).equals("\""))
						{
							StringBuffer sb = new StringBuffer( filter_target );
							sb.setCharAt(0, ' ');
							filter_target = sb.toString();
						}
						//last th character replace " '
						if(filter_target.substring(filter_target.length()-1, filter_target.length()).equals("'"))
						{
							StringBuffer sb = new StringBuffer( filter_target );
							sb.setCharAt(filter_target.length()-1, ' ');
							filter_target = sb.toString();
						}
						//last th character replace ' "
						if(filter_target.substring(filter_target.length()-1, filter_target.length()).equals("\""))
						{
							StringBuffer sb = new StringBuffer( filter_target );
							sb.setCharAt(filter_target.length()-1, ' ');
							filter_target = sb.toString();
						}
						filter_target = filter_target.trim();
						//logger.info(filter_target);
				        String[] target = filter_target.split("&|\\|");
				        for(int i = 0; i <target.length; i++)
				    	{
				        	String breath_rem = target[i].replace("(", "");
				        	breath_rem = breath_rem.replace(")", "");
				        	
				        	String[] col_val = breath_rem.split(",");				        	
				        	int col_idx = Integer.parseInt(col_val[0]);
				        	String  col_value = col_val[1];				        	
				        	if(hash_columnIncludeExcludeRule.containsKey(col_idx) == true)
				        	{
				        		List<String> ExcludeRule = hash_columnIncludeExcludeRule.get(col_idx);
				        		if(ExcludeRule.contains(col_value) == false)
				        		{ ExcludeRule.add(col_value);	}
				        	}
				        	else
				        	{
				        		List<String> ExcludeRule = new ArrayList<String>(); ExcludeRule.add(col_value);
				        		hash_columnIncludeExcludeRule.put(col_idx, ExcludeRule);
				        	}
				    	}
					}   
					else
					{
						//Rule File Scan...
						Path path = new Path(filter_rule_path);
						BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
						String line = "";
						//Start scan
						while ((line = in.readLine())!= null)
						{
							filter_target += line.trim();
							String[] target = line.split("&|\\|");
							//logger.info(line);
					        for(int i = 0; i <target.length; i++)
					    	{
					        	
					        	String breath_rem = target[i].replace("(", "");
					        	breath_rem = breath_rem.replace(")", "");	
					        	String[] col_val = breath_rem.split(",");				  
					        	int col_idx = Integer.parseInt(col_val[0]);
					        	String  col_value = col_val[1];				        	
					        	if(hash_columnIncludeExcludeRule.containsKey(col_idx) == true)
					        	{
					        		List<String> ExcludeRule = hash_columnIncludeExcludeRule.get(col_idx);
					        		if(ExcludeRule.contains(col_value) == false)
					        		{ ExcludeRule.add(col_value);	}
					        	}
					        	else
					        	{
					        		List<String> ExcludeRule = new ArrayList<String>(); ExcludeRule.add(col_value);
					        		hash_columnIncludeExcludeRule.put(col_idx, ExcludeRule);
					        	}
					    	}
						}
						in.close();
					}
			        break;
			}
			
	    }
    	catch(Exception e)
    	{
    		logger.info(e.toString());
    	}
    }
    /**
     * && 혹은 ||로 구분되는 논리식의 갯수를 리턴한다.
     * @author HongJoong.Shin
     * @date   2016.12.06
     * @return 논리식의 갯수
     */
    private int check_operator_dual(String input)
    {
    	int count = 0;
    	for(int i = 0; i < input.length(); i++)
    	{
    		String view = input.substring(i, i+1);
    		if(view.equals("<") == true||	view.equals("<=") == true||	view.equals(">") == true||view.equals(">=") == true)
    		{
    			count++;
    		}
    	}
    	return count;    	
    }
    
    int line = 0;
    /**
     * 입력 스프릿 데이터를 설정된 ETL 기능에 맞추어 레코드 제거, 컬럼 추출, 데이터 변환 작업을 수행한다.
     * @author HongJoong.Shin
     * @date   2016.12.06
	 * @param  Object key : 입력 스프릿 오프셋
	 * @param  Text value : 입력 스프릿 
	 * @param  Context context : 하둡 콘텍스트 정보 
	 * @return void
     */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		filter_target_cp = filter_target;
		String[] columns = value.toString().split(mDelimiter);	
		
		//원본 데이터의 컬럼수를 초과하는 경우. 사전에 0으로 처리한다.
		ETL_T_Method method = ETL_T_Method.valueOf(filter_method);
		boolean bNoRuleforFilterInEx = true;
		switch(method) 
		{
			/**
	         * 특정 컬럼의 특정 값을 갖는 레코드를 제거한다.
	         * @author HongJoong.Shin
	         * @date   2016.12.06
	         */
		    case FilterExclude:
	    	/**
	         * 특정 컬럼의 특정 값을 갖는 레코드만 추출한다.
	         * @author HongJoong.Shin
	         * @date   2016.12.06
	         */
		    case FilterInclude:		
				Set<Integer>col_set = hash_columnIncludeExcludeRule.keySet();
				for(int col : col_set)
				{
					if(col < 0)
						continue;
					if(col >= columns.length)
					{
						if(hash_columnIncludeExcludeRule.containsKey(col) == true)
			        	{
			        		List<String> ExcludeRule = hash_columnIncludeExcludeRule.get(col);
			        		        		
		        			for(String contrule: ExcludeRule)
		        			{
		        				String match_rule = "";
		        				match_rule = col + "," + contrule;
		        				bNoRuleforFilterInEx = false;
		        				filter_target_cp = filter_target_cp.replaceFirst(match_rule, "0");		    
		        			}
			        	}
					}
				}
				break;
		}
		if(bNoRuleforFilterInEx == false)
		{
			context.write(NullWritable.get(), new Text(""));
		}
		for(int col_num = 0;  col_num < columns.length; col_num++)
		{			
			method = ETL_T_Method.valueOf(filter_method);
			String  clmn_value = "";
			switch(method) 
			{
				/**
		         * 수치 데이터 정규화 처리.
		         * @author HongJoong.Shin
		         * @date    2016.12.06
		         */
				case NumericNorm:
					try
					{
			    		String targetNum = columns[col_num];
						//logger.info("check " + targetNum);
						if(isStringDouble(targetNum) == true)
						{
							
							ScriptEngineManager mgr = new ScriptEngineManager();
						    ScriptEngine engine = mgr.getEngineByName("JavaScript");
						    
						    Set<String> rules = hash_EvalNumericNorm.keySet();
						    boolean rtnEvaluation = false;
						    String matched_Rule = "";
						    
						    for(String rule: rules)
						    {
						    	matched_Rule = rule;
						    	if(rule.indexOf("x") >= 0)
						    	{
						    	   if(check_operator_dual(rule) == 2)
						    	   {
						    		   rule = rule.replace("x","x&&x"); //dual case   
						    	   }
						    	   rule = rule.replace("x", targetNum);
						    	  
						    	}
						    	if(rule.indexOf("X") >= 0)
						    	{
						    		if(check_operator_dual(rule) == 2)
						    		{
						    		   rule = rule.replace("X","X&&X"); //dual case   
						    		}
						    		rule = rule.replace("X", targetNum);
						    	
						    	}
						    	String evaluation = rule;
						    	try
						    	{
						    		//logger.info("평가: " + evaluation);
							    	rtnEvaluation = (Boolean) engine.eval(evaluation);
							    	if(rtnEvaluation == true)
							    	{
							    		//logger.info("True");
							    		
							    		break; //범위가 맞을 경우 루프 아웃.
							    	}
						    	}
						    	catch(Exception e)
						    	{
						    		//logger.info(e.toString());
						    	}
						    }						   
						    /*
							 * 20170928 HongJoong.Shin
							 * 선택한 컬럼 번호에 해당할 경우만 변환 수행. 
							 */
							if(CommonMethods.isContainIndex(indexArray, col_num, true) && 
					    			!CommonMethods.isContainIndex(exceptionIndexArr, col_num, false))
							{	
							    if(rtnEvaluation == true)
							    {
							    	columns[col_num] = hash_EvalNumericNorm.get(matched_Rule);		
							    }
							}
						}
						replaced_key = replaced_key + columns[col_num] + mDelimiter;
					}
					catch(Exception e)
					{
						//logger.info(e.toString());
					}
				    break;
				    /**
			         * 조건에 부합 하는 문자열을 입력 문자열로 변경.
			         * @author HongJoong.Shin
			         * @date   2016.12.06
			         */
			    case Replace:
			    	if(hash_columnReplaceRule.containsKey(col_num) == true)
		    		{
			    		HashMap<String, String> replaceRule = hash_columnReplaceRule.get(col_num);
			    		String sourcePattern = columns[col_num];
			    		
			    		Set<String> rules = replaceRule.keySet();
			    		for(String regex: rules)
			    		{
			    				
		    				if(Pattern.matches(regex, sourcePattern) == true)
		    				{
		    					String repCode = replaceRule.get(regex);
	    						columns[col_num] = columns[col_num].replaceAll(sourcePattern, repCode);	
		    				}
			    		}
			    		replaced_key = replaced_key + columns[col_num] + mDelimiter;
		    		}
			    	else
			    	{
			    		replaced_key = replaced_key + columns[col_num] + mDelimiter;
			    	}
			    	break;
			    	/**
			         * 특정 컬럼만 추출함.
			         * @author HongJoong.Shin
			         * @date   2016.12.06
			         */
			    case ColumnExtractor:
			    	if(CommonMethods.isContainIndex(indexArray, col_num, true) && 
			    			!CommonMethods.isContainIndex(exceptionIndexArr, col_num, false))
					{	
			    		replaced_key = replaced_key + columns[col_num] + mDelimiter;
			    		
					}
			    	break;
			    	/**
			         * 특정 컬럼의 특정 값을 갖는 레코드를 제거한다.
			         * @author HongJoong.Shin
			         * @date   2016.12.06
			         */
			    case FilterExclude:
			    	/**
			         * 특정 컬럼의 특정 값을 갖는 레코드만 추출한다.
			         * @author HongJoong.Shin
			         * @date   2016.12.06
			         */
			    case FilterInclude:				    	
			    	String match_rule = "";
			    	if(hash_columnIncludeExcludeRule.containsKey(col_num) == true)
		        	{
		        		List<String> ExcludeRule = hash_columnIncludeExcludeRule.get(col_num);
		        		for(String rule: ExcludeRule)
		        		{
		        			if(columns[col_num].equals(rule) == true)
		        			{
		        				match_rule = col_num + "," + columns[col_num];
		        				filter_target_cp = filter_target_cp.replaceFirst(match_rule, "1");
		        			}
		        			else
		        			{
		        				match_rule = col_num + "," + rule;
		        				filter_target_cp = filter_target_cp.replaceFirst(match_rule, "0");		 
		        			}
		        		}
		        	}			    
			    	extrudded_value += columns[col_num] + mDelimiter;
			    	break;
			default:
				break;		    	
			}
		}
		
		int result = 0;
		method = ETL_T_Method.valueOf(filter_method);
		Calculator c = new Calculator();
        String postfixExpression;
		switch(method) 
		{
			/**
	         * 특정 컬럼의 특정 값을 갖는 레코드만 추출한다.
	         * @author HongJoong.Shin
	         * @date    2016.12.06
	         */
		    case FilterInclude:		    	
		    	if(filter_target_cp.length() == 1)
		    	{
		    		if(filter_target_cp.equals("1") == true)
		    		{
		    			extrudded_value = extrudded_value.substring(0, extrudded_value.length() -1);
		    			context.write(NullWritable.get(), new Text(extrudded_value));
		    		}
		    	}
		    	else
		    	{
			        postfixExpression = c.getPostfix(filter_target_cp);   
			        result = c.calculate(postfixExpression);// 계산
			        if(extrudded_value.trim().length() > 0 && result == 1)
					{
			        	int linenum =  line++;
			        	extrudded_value = extrudded_value.substring(0, extrudded_value.length() -1);
						context.write(NullWritable.get(), new Text(extrudded_value));
					}
		    	}
		    	extrudded_value = "";
				filter_target_cp = "";
				break;
			/**
	         * 특정 컬럼의 특정 값을 갖는 레코드를 제거한다.
	         * @author HongJoong.Shin
	         * @date    2016.12.06
	         */
		    case FilterExclude:		    			
		    	if(filter_target_cp.length() == 1)
		    	{
		    		if(filter_target_cp.equals("1") == false)
		    		{
		    			extrudded_value = extrudded_value.substring(0, extrudded_value.length() -1);
		    			logger.info(extrudded_value);
		    			context.write(NullWritable.get(), new Text(extrudded_value));
		    		}
		    	}
		    	else
		    	{
			        postfixExpression = c.getPostfix(filter_target_cp);   
			        // 계산
			        result = c.calculate(postfixExpression);
			        if(extrudded_value.trim().length() > 0 && result != 1)
					{
			        	int linenum =  line++;
						extrudded_value = extrudded_value.substring(0, extrudded_value.length()-1);
						logger.debug(extrudded_value);
						context.write(NullWritable.get(), new Text(extrudded_value));
						
					}
		    	}
		    	extrudded_value = "";
				filter_target_cp = "";
				break;
			/**
	         * 특정 컬럼만 추출함.
	         * @author HongJoong.Shin
	         * @date    2016.12.06
	         */
		    case ColumnExtractor:
	    	/**
	         * 수치 데이터 정규화 처리.
	         * @author HongJoong.Shin
	         * @date    2016.12.06
	         */
		    case NumericNorm:
	    	/**
	         * 조건에 부합 하는 문자열을 입력 문자열로 변경.
	         * @author HongJoong.Shin
	         * @date    2016.12.06
	         */
		    case Replace:
				if(replaced_key.trim().length() > 0)
				{
					replaced_key = replaced_key.substring(0, replaced_key.length()-1);
					logger.debug(replaced_key);
					context.write(NullWritable.get(), new Text(replaced_key));
					replaced_key = "";
				}
				break;
		}
		
	}//end of mapper
}
