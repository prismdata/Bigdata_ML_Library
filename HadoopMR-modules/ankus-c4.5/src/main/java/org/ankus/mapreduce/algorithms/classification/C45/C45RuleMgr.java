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
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: moonie
 * Date: 14. 6. 5
 * Time: 오후 6:34
 * To change this template use File | Settings | File Templates.
 */
public class C45RuleMgr {
	private Logger logger = LoggerFactory.getLogger(C45RuleMgr.class);
    public String loadNonLeafNode(Configuration conf) throws Exception
    {
        String nodeStr = null;
        if(conf.get(ArgumentsConstants.RULE_PATH)==null) return "root";
        Path ruleFilePath = new Path(conf.get(ArgumentsConstants.RULE_PATH));
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fin = fs.open(ruleFilePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

        String readStr;
        br.readLine();
        while((readStr = br.readLine())!=null)
        {
        	String isTerminal = readStr.substring(readStr.length() - 5);
            if((readStr.length() > 0) && isTerminal.equals("false"))
            {
                nodeStr = readStr;
                break;
            }
        }
        br.close();
        fin.close();
        return nodeStr;
    }
   
    public int updateRule(Configuration conf, String oldRulePath, String ExpensionSrcRule) throws Exception
    {
        // rule selecting 
        RuleNodeBaseInfo[] nodes = getSelectedNodes_ClassDist1(conf);
        if(nodes == null)
        {
        	logger.error("No new nodes");
        	return 1;
        }
        else
        {
        	 // rule write
	        //System.out.println("Node created");
	        String delimiter = conf.get(ArgumentsConstants.DELIMITER);
	        writeRules(conf, nodes, delimiter, oldRulePath, ExpensionSrcRule);
	        return 0;
        }
    }
    private void writeRules(Configuration conf, RuleNodeBaseInfo[] nodes, String delimiter, String oldRulePath, String ruleStr) throws Exception
    {
        FileSystem fs = FileSystem.get(conf);

        if(ruleStr.equals("root"))
        {
            FSDataOutputStream fout = fs.create(new Path(conf.get(ArgumentsConstants.RULE_PATH)), true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
            
            bw.write("# [AttributeName-@@Attribute-Value][@@].., Data-Count, Node-Purity, Class-Label, Is-Leaf-Node" + "\n");
//            logger.info("# [AttributeName-@@Attribute-Value][@@].., Data-Count, Node-Purity, Class-Label, Is-Leaf-Node");
            for(int i=0; i<nodes.length; i++)
            {
            	String root = nodes[i].toString(delimiter);
            	//System.out.println(root);
            	bw.write(root + "\n");
//            	logger.info(root);
            }
            bw.close();
            fout.close();
        }
        else
        {
        	try
	        {
	            FSDataInputStream fin = FileSystem.get(conf).open(new Path(oldRulePath));
	            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
	
	            FSDataOutputStream fout = fs.create(new Path(conf.get(ArgumentsConstants.RULE_PATH)), true);
	            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
	            String readStr;
	            while((readStr=br.readLine())!=null)
	            {
	                if(readStr.equals(ruleStr))
	                {
	                    if(nodes.length > 1)
	                    {
	                    	//System.out.println(readStr+ "-cont");
	                        bw.write(readStr + "-cont\n");
	                        String[] history = readStr.split(delimiter);
	                        String false_previous_rule = "";
	                        for(int i=0; i<history.length-4; i++)//Rule Append
	                        {
	                        	false_previous_rule += history[i] + ",";
	                        }
	                        false_previous_rule = false_previous_rule.substring(0, false_previous_rule.length()-1);
	                        for(int i=0; i<nodes.length; i++)//Rule Append
	                        {
	                        	bw.write(false_previous_rule + "@@" + nodes[i].toString(delimiter) + "\n");
//	                        	logger.info(false_previous_rule + "@@" + nodes[i].toString(delimiter));
	                        }
	                    }
	                    else
	                    {
	                		//System.out.println(readStr + "-true");
	                    	bw.write(readStr + "-true");
//	                    	logger.info(readStr + "-true");
	                    }
	                }
	                else 
	                {
	                	//System.out.println(readStr);
	                	bw.write(readStr + "\n");
//	                	logger.info(readStr + "-true");
	                }
	            }
	            br.close();
	            fin.close();
	            bw.close();
	            fout.close();
	        }
        	catch(Exception e)
        	{
        		//System.out.println(e.toString());
        	}
        }
    }
    

    public Double calculateShannonEntropy(List<String> values) 
    {
    	  Map<String, Integer> map = new HashMap<String, Integer>();
    	  // count the occurrences of each value
    	  for (String sequence : values) 
    	  {
    	    if (!map.containsKey(sequence))
    	    {
    	      map.put(sequence, 0);
    	    }
    	    map.put(sequence, map.get(sequence) + 1);
    	  }
    	 
    	  // calculate the entropy
    	  Double result = 0.0;
    	  for (String sequence : map.keySet())
    	  {
    	    Double frequency = (double) map.get(sequence) / values.size();
    	    result -= frequency * (Math.log(frequency) / Math.log(2));
    	  }
    	 
    	  return result;
    }
    
    private double Get_PartitionPoint(HashMap<Double , List<String>> value_classMap)
    {
    	double point = 0.0;
    	
    	Iterator<Double> valudIterator_PI = value_classMap.keySet().iterator();
    	Iterator<Double> valudIterator = value_classMap.keySet().iterator();
    	while(valudIterator_PI.hasNext())
    	{
    		double value_pi = valudIterator_PI.next();
    		List<String> Less_side  = new ArrayList<String>();
    		List<String> Bigger_side  = new ArrayList<String>();
    		while(valudIterator.hasNext())
        	{
    			double value = valudIterator.next();
    			if(value_pi > value)
    			{
    				List<String> tmp = value_classMap.get(value);
    				Less_side.addAll(tmp);
    			}
    			else
    			{
    				List<String> tmp = value_classMap.get(value);
    				Bigger_side.addAll(tmp);
    			}
        	}
    		//System.out.println("Less classes : " + Less_side.toString());
    		//System.out.println("Bigger classes : " + Bigger_side.toString());
    		double L_Entropy = calculateShannonEntropy(Less_side);
    		double R_Entropy = calculateShannonEntropy(Bigger_side);
    		//System.out.println("Less Entropy : " + L_Entropy);
    		//System.out.println("Bigger Entropy : " + R_Entropy);
    	}
    	return point;
    }
    
    private RuleNodeBaseInfo[] getSelectedNodes_ClassDist1(Configuration conf)
    {
    	RuleNodeBaseInfo[] retNodes = null;
    	String distrubition = "";
    	try
    	{	
    		
			double MaxGainRatio = -1 * 9999.0;
			double averageInfoGain = 0;
			int attribute_index = 0;
			String readStr = null;
			Path entropyPath = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH));
			FileStatus[] FileStatus = FileSystem.get(conf).listStatus(entropyPath);
			List<Double> infoGainList = new ArrayList<Double>();
			List<Double> GainRaioList = new ArrayList<Double>();
			List<String> ClassDistribution = new ArrayList<String>();
			List<Integer> Attribute_Idx = new ArrayList<Integer>();
			String[] sp_clsid_class_dist = null;
			String  m_delimiter = conf.get(ArgumentsConstants.DELIMITER, "\t");
			int numAttribute = 0;
			try
			{
		    	for(int i=0; i<FileStatus.length; i++)
				{
					if(!FileStatus[i].getPath().toString().contains("part-r-")) continue;
					//System.out.println("GainRatio Path:" + FileStatus[i].getPath());
					FSDataInputStream fin = FileSystem.get(conf).open(FileStatus[i].getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
					
					while((readStr = br.readLine())!=null)
					{	
//						System.out.println(">>>>Distribution:" + readStr);
						String[] key_n_Dist = readStr.split(m_delimiter);					
						String[] curInfoGain = key_n_Dist[1].split("\u0000");
											
						averageInfoGain += Double.parseDouble(curInfoGain[0]);
						numAttribute++;
						String[] GainRatio = curInfoGain[1].split("\u0001");
						double dblGainRatio = Double.parseDouble(GainRatio[0]);			
						Attribute_Idx.add(Integer.parseInt(key_n_Dist[0]));
						infoGainList.add(Double.parseDouble(curInfoGain[0]));
						GainRaioList.add(Double.parseDouble(GainRatio[0]));
						ClassDistribution.add(GainRatio[1]);
					}
					br.close();
					fin.close();
				}
			}
			catch(Exception e)
			{
				logger.error("Rule File Load Exception: " + e.toString());
				return null;
			}
	    	averageInfoGain = averageInfoGain / numAttribute;
	    	double minResult = 0.0;
	    	for(int i = 0; i < numAttribute; i++)
	    	{
	    		if((infoGainList.get(i) >= (averageInfoGain - 1E-3)) && (GainRaioList.get(i) > minResult))
				{
	    			minResult = GainRaioList.get(i);
	    			
	    			attribute_index = Attribute_Idx.get(i);
	    			//클래스 분포 추가
	    			distrubition = ClassDistribution.get(i);
				}
	    	}
	    	
	    	if(distrubition == "")
	    	{
	    		//System.out.println("Error Null Node");
	    		return null;
	    	}
    		String[] dataType =  distrubition.split("\u0007");
	    	//dataType[1]에 \u0004기준으로 원본 데이터 존재함.
    		
			//CHECK NUM OR CHARACTER
			if(dataType[0].equals("C") == true)//CONTROL Norminal Value
			{
				sp_clsid_class_dist = dataType[1].split("\u0003");
				retNodes = new RuleNodeBaseInfo[sp_clsid_class_dist.length-1];
				for(int clsid = 1; clsid < sp_clsid_class_dist.length; clsid++)
				{
					String SPV = "";
					String[] splitValue = sp_clsid_class_dist[clsid].split("##");
					
					SPV = splitValue[0];
					String[] class_dist = splitValue[1].split("\u0002");
					
					if(class_dist.length == 1)
					{
						int data_count = 0;
						String[] data_distribution = class_dist[0].split("::");
						data_count += Integer.parseInt(data_distribution[1]);
						//System.out.println(SPV + "-" +data_distribution[0] + " Terminal");
						retNodes[clsid-1] = new RuleNodeBaseInfo(attribute_index+"", SPV , data_count, 0.0 , data_distribution[0]);
						retNodes[clsid-1].setIsLeaf(true);
					}
					else
					{
						int data_count = 0;
						String[] data_distribution = null;
						for(int cdi = 0; cdi < class_dist.length; cdi++)
						{
							data_distribution = class_dist[cdi].split("::");
							data_count += Integer.parseInt(data_distribution[1]);
						}
						//System.out.println(SPV + "-" + " NonTerminal");
						int min_leaf = conf.getInt(ArgumentsConstants.MIN_LEAF_DATA, 2);
						retNodes[clsid-1] = new RuleNodeBaseInfo(attribute_index+"", SPV, data_count, 0.0 ,data_distribution[0]);
						if(data_count < min_leaf)
						{
							retNodes[clsid-1].setIsLeaf(true);
						}
						else
						{
							retNodes[clsid-1].setIsLeaf(false);
						}
					}
				}
			}
			else if(dataType[0].equals("N") == true)//데이터 타입(수치)과 분리지점 후보, 원본 데이터로 구성된 문자열 
			{
				int rawDataStart = dataType[1].indexOf("\u0004"); 
	    		dataType[1] = dataType[1].substring(0, rawDataStart); 
	    		//System.out.println(dataType[1]);
	    		
				retNodes = new RuleNodeBaseInfo[2];
				
				sp_clsid_class_dist = dataType[1].split("##");
				
				String splitValue = sp_clsid_class_dist[0]; //분리 후보 추출.
				
				for(int clsid = 1; clsid < sp_clsid_class_dist.length; clsid++)
				{
					String tmp = sp_clsid_class_dist[clsid]; //수치 데이터 규칙 추출.(크기 포함.)
					String[] scale = tmp.split("\u0003"); 
					
					//If NODE_dist is > 1 non Terminal 
					//Else Terminal
					String[] NODE_Distribution = scale[1].split("\u0002");
					int idRetNode = 0;
					if(scale[0].equals("<") == true)
					{
						idRetNode = 0;
					}
					else
					{
						idRetNode = 1;
					}
					String newNode = "";
					if(splitValue.contains("@@"))
					{
						String[] rowValue = splitValue.split("@@");
						String targetSV = rowValue[rowValue.length-1];						
						String new_targetSV = targetSV.replace(targetSV, scale[0] + "&&"+ targetSV);						
						rowValue[rowValue.length-1] = rowValue[rowValue.length-1].replace(targetSV, new_targetSV);
						
						for(int rvi = 0; rvi < rowValue.length; rvi++)
						{
							newNode += rowValue[rvi] + "@@";
						}
						newNode = newNode.substring(0, newNode.length()-2);
					}
					else
					{
						String[] rawData = distrubition.split("\u0004");
						String[] rawValue = rawData[1].split("\u0005");
						double newSplitPoint = -Double.MAX_VALUE;
					    double originalValue;
					    for(int ri = 0; ri < rawValue.length; ri++)
					    {
					    	originalValue  = Double.parseDouble(rawValue[ri]);
					    	if (( originalValue > newSplitPoint)  && ( originalValue <= Double.parseDouble(splitValue))) 
					    	{
					    		newSplitPoint =  originalValue;
					    	}
					      }
					      //System.out.println("splitValue:" + newSplitPoint);
					      splitValue = newSplitPoint+"";
					      newNode = scale[0] + "&&" + splitValue;
					}
					
					if(NODE_Distribution.length > 1)
					{
						//NonTerminal
						int min_zero_count = 0;
						int data_count = 0;
						String class_Label = "";
						for(int ti = 0; ti < NODE_Distribution.length; ti++)
						{
							//System.out.println("Node "+ ti + " Distributin:" + NODE_dist[ti]);
							String[] treeCount = NODE_Distribution[ti].split("::"); 
							int cls_dtCount = Integer.parseInt(treeCount[1]);
							data_count += cls_dtCount;
							class_Label = treeCount[0];
						}
						//Non-Terminal
						//String attrCondition, String attrValue, int dataCnt, double purity, String classLabel
						retNodes[idRetNode] = new RuleNodeBaseInfo(attribute_index+"", 
																	newNode, 
																	data_count, 
																	0.0, 
																	class_Label);
						
						int min_leaf = conf.getInt(ArgumentsConstants.MIN_LEAF_DATA, 2);
						if(data_count < min_leaf)
						{
							retNodes[idRetNode].setIsLeaf(true);
						}
						else
						{
					        retNodes[idRetNode].setIsLeaf(false);
						}
					}
					else
					{
						//Terminal
						//System.out.println("SubTree id: " + clsid);
						int data_count = 0;
						String class_Label = "";
						for(int ti = 0; ti < NODE_Distribution.length; ti++)
						{
							//System.out.println("Node "+ ti + " Distributin:" + NODE_dist[ti]);
							String[] treeCount = NODE_Distribution[ti].split("::"); 
							int cls_dtCount = Integer.parseInt(treeCount[1]);
							data_count += cls_dtCount;
							class_Label = treeCount[0];
						}
						retNodes[idRetNode] = new RuleNodeBaseInfo(attribute_index+"", 
																	newNode, 
																	data_count,
																	0.0,
																	class_Label);
						
						retNodes[idRetNode].setIsLeaf(true);
						
					}
				}
			}
    	}
    	catch(Exception e)
    	{
    		logger.info(e.toString());
    	}
    	
    	return retNodes;
    }
    
//    private RuleNodeBaseInfo[] getSelectedNodes_(Configuration conf) throws Exception
//    {
//        String delimiter = conf.get(ArgumentsConstants.DELIMITER);
//        Path entropyPath = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH));
//        FileStatus[] status = FileSystem.get(conf).listStatus(entropyPath);
//
//        String selectedStr = "";
//        double minEntropy = 9999.0;
//        double maxGainRatio = Double.MIN_VALUE;
//        
//        for(int i=0; i<status.length; i++)
//        {
//            if(!status[i].getPath().toString().contains("part-r-")) continue;
//
//            FSDataInputStream fin = FileSystem.get(conf).open(status[i].getPath());
//            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));            
//            String readStr;            
//            while((readStr = br.readLine())!=null)
//            {
//                double curEntropy = Double.parseDouble(readStr.split(delimiter)[1]);                
//                if(minEntropy > curEntropy)
//                {
//                    minEntropy = curEntropy;
//                    selectedStr = readStr;
//                }
//            }           
//            br.close();
//            fin.close();
//        }   
//        String tokens[] = selectedStr.split(delimiter);
//        //System.out.println(selectedStr);
//        int attrCnt = (tokens.length - 2) / 4;    // index, entropy and 4-set
//        RuleNodeBaseInfo[] retNodes = new RuleNodeBaseInfo[attrCnt];
//        int minDataCnt = Integer.parseInt(conf.get(ArgumentsConstants.MIN_LEAF_DATA));
//        double minPurity = Double.parseDouble(conf.get(ArgumentsConstants.PURITY)); 
//        for(int i=0; i<attrCnt; i++)
//        {
//            int base = i * 4;
//            
//            int dataCnt = Integer.parseInt(tokens[base+3]);
//            double purity = Double.parseDouble(tokens[base+4]);
//            String classlbl = tokens[base+5];
//            String attrValue = tokens[base+2] ;
//            String attrCondition = tokens[0];
//           
//            retNodes[i] = new RuleNodeBaseInfo(attrCondition, attrValue, dataCnt, purity , classlbl);    
//            
//            if((minDataCnt >= dataCnt) ||(minPurity < purity)) 
//        	{     	
//            	retNodes[i].setIsLeaf(true);   	
//            }
//            else 
//        	{    
//            	retNodes[i].setIsLeaf(false);   
//            }        	
//        }
//        return retNodes;
//    }
//    double split_point = 0.0;
//    double criteria = 0.0;
//    double MDL_gain = 0.0;
//    boolean leftsubset = false;
//    int FTi = 0;
    
    public static boolean isStringDouble(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
      }
    
    
}
