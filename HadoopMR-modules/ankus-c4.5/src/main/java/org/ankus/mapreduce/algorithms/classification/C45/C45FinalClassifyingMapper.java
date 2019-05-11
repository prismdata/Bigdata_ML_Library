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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

import org.slf4j.LoggerFactory;
/**
 * ID3FinalClassifyingMapper
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.12
 * @author Moonie Song
 */
public class C45FinalClassifyingMapper extends Mapper<Object, Text, NullWritable, Text>{

	String m_delimiter;
    String m_subDelimiter;
    ArrayList<String[]> ruleConditionList;
    ArrayList<String> classList;
	int mIndexArr[];                     // index array used as clustering feature
	int mNominalIndexArr[];              // index array of nominal attributes used as clustering features
	int mExceptionIndexArr[];            // index array do not used as clustering features
    int m_classIndex;
    private org.slf4j.Logger logger = LoggerFactory.getLogger(C45FinalClassifyingMapper.class);
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
        m_subDelimiter = RuleNodeBaseInfo.conditionDelimiter;

        FileSystem fs = FileSystem.get(conf);
        Path ruleFilePath = new Path(conf.get(ArgumentsConstants.RULE_PATH));

        ruleConditionList = new ArrayList<String[]>();
        classList = new ArrayList<String>();
        loadRuleList(fs, ruleFilePath, m_delimiter, m_subDelimiter);
        
        mIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX,  "-1"));
		mNominalIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX,  "-1"));
		mExceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
    }

	@Override
	protected void map(Object key, Text value, Context context) //hrows IOException, InterruptedException
	{
		try
		{
        String[] columns = value.toString().split(m_delimiter);

        int ruleIndex = -1;
        String classStr = "";
        int preMatchCnt = 0;
        for(int i=0; i<ruleConditionList.size(); i++)
        {
            String[] condList = ruleConditionList.get(i);
            int condCnt = condList.length/2;
            
            int matchCnt = isMatchCnt(condList, condCnt, columns);
            if(matchCnt == condCnt)
            {
                classStr = classList.get(i);
                ruleIndex = i;
            }

            if(matchCnt < preMatchCnt) break;
            preMatchCnt = matchCnt;
        }
        context.write(NullWritable.get(), new Text(value + m_delimiter + classStr));
		}
		catch(Exception e)
		{
			logger.error(e.toString());
			e.printStackTrace();
		}
	}
	public boolean isNumeric(String str)  
    {  
      try  
      {  
        double d = Double.parseDouble(str);  
      }  
      catch(NumberFormatException nfe)  
      {  
        return false;  
      }  
      return true;  
    }
    private int isMatchCnt(String[] conditionArr, int condCnt, String[] dataAttrArr)
    {
    	int matchCnt = 0;

//        int isNumber = 0;
    	int isNumber = -1; //0혹은 1도 중첩 되어서는않됨.
        for(int i=0; i<condCnt; i++)
        {
				int attrIndex = Integer.parseInt(conditionArr[i*2]);
				//input index의 수치, 범주 설정 확인.
				
				if(!CommonMethods.isContainIndex(mExceptionIndexArr, attrIndex, false))
				{
					if(CommonMethods.isContainIndex(mNominalIndexArr, attrIndex, false))
					{
						isNumber = 0;
					}
					else if(CommonMethods.isContainIndex(mIndexArr, attrIndex, true))
					{
						isNumber = 1;
					}
				}
            	switch(isNumber)
				{
	            case 1:
	            {
	            	String [] rule= conditionArr[i*2+1].split("&&");
	            	System.out.println(dataAttrArr[attrIndex]);
		            double input = Double.parseDouble(dataAttrArr[attrIndex]);
		            double rule_cmp = Double.parseDouble(rule[1]);
		            
		            switch(rule[0])
		            {
		            case "<":
		            	if(input <= rule_cmp)
		            	{
		            		matchCnt++;
		            	}
		            	break;
		
		            case ">":
		            	if(input > rule_cmp)
		            	{
		            		matchCnt++;
		            	}
		            	break;
		          
		            }
		            break;
	            }
	            case 0:
	            {
	            	if(!dataAttrArr[attrIndex].equals(conditionArr[i*2+1]))
	            		break;
	                matchCnt++;	
	            }
            }
        }

        return matchCnt;
    }

    private void loadRuleList(FileSystem fs, Path rulePath, String delimiter, String subDelimiter) throws IOException
    {
        FSDataInputStream fin = fs.open(rulePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

        String readStr, tokens[];
        br.readLine();
        while((readStr=br.readLine())!=null)
        {
            tokens = readStr.split(delimiter);
            ruleConditionList.add(tokens[0].split(subDelimiter));
            classList.add(tokens[3]);
        }

        br.close();
        fin.close();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    }
}
