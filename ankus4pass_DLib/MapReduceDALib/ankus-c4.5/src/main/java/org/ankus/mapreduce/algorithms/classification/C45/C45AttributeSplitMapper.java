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

package org.ankus.mapreduce.algorithms.classification.C45;

import org.ankus.mapreduce.algorithms.classification.rulestructure.RuleNodeBaseInfo;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * C4.5 AttributeSplitMapper
 * @desc
 *
 * @version 0.1
 * @date : 2016.02.10
 * @author Shin Hoong Joong
 */

public class C45AttributeSplitMapper extends Mapper<Object, Text, Text, Text>{

    String m_delimiter;
    String m_ruleCondition;
	int mIndexArr[];                     // index array used as clustering feature
	int mNominalIndexArr[];              // index array of nominal attributes used as clustering features
	int mExceptionIndexArr[];            // index array do not used as clustering features
    int m_classIndex;
    HashMap<Integer, List<String>> m_conditionMap;
    boolean all_nomatch = true;
    String[] columns = null;
    private Logger logger = LoggerFactory.getLogger(C45AttributeSplitMapper.class);
    @Override
    protected void setup(Context context)
    {
        Configuration conf = context.getConfiguration();
        try
        {
	        
	        String class_idx = conf.get(ArgumentsConstants.CLASS_INDEX, "-1");
	        String numeric_idx = conf.get(ArgumentsConstants.TARGET_INDEX, "-1");
	        String norminal_idx = conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1");
	       
	        if(CommonMethods.convertIndexStr2IntArr(numeric_idx).length > 1)
	        {
	        	if(numeric_idx.contains(","+class_idx))
		        {
		        	numeric_idx = numeric_idx.replace(","+class_idx, "");
		        }
		        if(numeric_idx.contains(class_idx + ","))
		        {
		        	numeric_idx = numeric_idx.replace(class_idx + ",", "");
		        }
	        }
	        else
	        {
	        	if(numeric_idx.contains(class_idx))
		        {
		        	numeric_idx = numeric_idx.replace(class_idx, "-1");
		        }
	        }
	        if(CommonMethods.convertIndexStr2IntArr(norminal_idx).length > 1)
	        {
		        if(norminal_idx.contains(","+class_idx))
		        {
		        	norminal_idx = norminal_idx.replace(","+class_idx, "");
		        }
		        if(norminal_idx.contains(class_idx + ","))
		        {
		        	norminal_idx = norminal_idx.replace(class_idx + ",", "");
		        }
	        }
	        else
	        {
	        	if(norminal_idx.contains(class_idx))
		        {
		        	norminal_idx = norminal_idx.replace(class_idx, "-1");
		        }
	        }
			mIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
			mNominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
			mExceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));

	        m_classIndex = Integer.parseInt(conf.get(ArgumentsConstants.CLASS_INDEX, "-1"));
	        m_ruleCondition = conf.get(Constants.C45_RULE_CONDITION, "root");
	        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
	        
	        if(m_ruleCondition.equals("root"))
	    	{
	        	m_conditionMap = null;
	    	}
	        //Use generated Rule
	        else
	        {
	            m_conditionMap = new HashMap<Integer, List<String>>();
	            String tokens[] = m_ruleCondition.split(RuleNodeBaseInfo.conditionDelimiter);
	            for(int i=0; i<tokens.length; i++)
	            {
	            	logger.info("Rule-Input:" +tokens[i]);
	            	int fidx = Integer.parseInt(tokens[i]);
	            	String str_rule = tokens[i+1];
	            	List<String> list_rule  = null;
	            	if(m_conditionMap.containsKey(fidx))
        			{
	            		list_rule = m_conditionMap.get(fidx);
	            		if(!list_rule.contains(str_rule))
	            			list_rule.add(str_rule);
        			}
	            	else
	            	{
	            		list_rule = new ArrayList<String>();
	            		list_rule.add(str_rule);
	            	}
	            	m_conditionMap.put(fidx, list_rule);
	                i++;
	            }
//	            logger.info("Rule List:" + m_conditionMap.toString());
	        }
	        
        }
        catch(Exception e)
        {
        	logger.error(e.toString());
        }
    }

	@Override
	protected void map(Object key, Text value, Context context)
	{
        try
        {
        	columns = value.toString().split(m_delimiter);
            if(columns.length <= 1 )
            {
            	Exception indexException = new Exception ("Please check delimiter or Variable Length");
            	throw indexException;
            }
	        //When rule file is not exist
	        if(m_conditionMap==null)
	        {
	            for(int i=0; i<columns.length; i++)
	            {
//	            	if(CommonMethods.isContainIndex(mNominalIndexArr, i, true)  && !CommonMethods.isContainIndex(mIndexArr, i, false)  && !CommonMethods.isContainIndex(mExceptionIndexArr, i, false) && (i!=m_classIndex))
	            	if( !CommonMethods.isContainIndex(mExceptionIndexArr, i, false) && (i!=m_classIndex))
	                {
	                    // attr-index, attr-value, class-value                	
	                    context.write(new Text(i + ""), new Text(columns[i] + m_delimiter + columns[m_classIndex]));
	                }
	            }
	        }
	        //When rule file exist
	        else
	        {
	            Iterator keyIter = m_conditionMap.keySet().iterator();
	            //System.out.println(m_conditionMap.toString());
	            boolean match = true;//Pass with on condition match
	            int keyAttrIndex  = 0;
	            while(keyIter.hasNext())
	            {
	                keyAttrIndex = (Integer)keyIter.next();
	                
	                List<String> line_Condition =  m_conditionMap.get(keyAttrIndex); 
	                for(String condition: line_Condition)
	                {
		                String rule_condition =condition;
		                
		                double inputNum = 0.0, ruleNum = 0.0;
		                String[] sign  = null;
		                if(rule_condition.indexOf("&&") > 0)//Numeric case
		                {
		                	sign  = rule_condition.split("&&");
		                	inputNum = Double.parseDouble(columns[keyAttrIndex]);
		                	ruleNum = Double.parseDouble(sign[1]);
		                	
		                	switch(sign[0])
		                	{
		                		case "<":
		                			if( inputNum <= ruleNum)
		                			{
		                			}	                	
		                			else
		                			{
		                				match = false;
		                			}
		                			break;
		                		
		                		case ">":
		                			if(inputNum > ruleNum)
		                			{	
	
		                			}
		                			else
		                			{
		                				match = false;
		                			}
		                			break;
		                		default:
		                			logger.info("unknown condition");
		                			break;
		                	}
		                }
		                else //Norminal case
		                {
		                	//하나라도 규칙과 다르면 match fail
		                	if(rule_condition.equals(columns[keyAttrIndex]) == false)
		                	{
		                		match = false;
		                	}
		                }
	                }
	            }
	            if(match) //Only read Non Terminal
	            {
					//column 단위 처리 
					for(int ci = 0;  ci < columns.length;  ci ++)
					{
//						if(!CommonMethods.isContainIndex(mExceptionIndexArr, ci, false))
						if( !CommonMethods.isContainIndex(mExceptionIndexArr, ci, false) && (ci!=m_classIndex))
						{
							all_nomatch = false;
							String atr_idx = ci+"";
							String map_value = 	m_ruleCondition + 
									 						RuleNodeBaseInfo.conditionDelimiter + 
									 						columns[ci] + 
									 						m_delimiter + 
									 						columns[m_classIndex];
//							logger.info(atr_idx + "" + map_value);
							context.write(new Text(atr_idx + ""), new Text(map_value));
						}
					}
	            }
	         }
        }
        catch(Exception e)
        {
        	logger.error(e.toString());        	
        }
	}


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    	
    }
}
