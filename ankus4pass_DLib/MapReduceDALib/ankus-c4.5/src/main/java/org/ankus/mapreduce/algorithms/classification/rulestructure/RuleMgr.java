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
package org.ankus.mapreduce.algorithms.classification.rulestructure;

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

/**
 * Created with IntelliJ IDEA.
 * User: moonie
 * Date: 14. 6. 5
 * Time: 오후 6:34
 * To change this template use File | Settings | File Templates.
 */
public class RuleMgr {
	private Logger logger = LoggerFactory.getLogger(RuleMgr.class);
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
   
    public void updateRule(Configuration conf, String oldRulePath, String ruleStr) throws Exception
    {
        // rule selecting    	
        RuleNodeBaseInfo[] nodes = getSelectedNodes(conf);
        System.out.println("Node created");
        // rule write
        String delimiter = conf.get(ArgumentsConstants.DELIMITER);
        writeRules(conf, nodes, delimiter, oldRulePath, ruleStr);
        System.out.println("Rule Writed");
    }
    private void writeRules(Configuration conf, RuleNodeBaseInfo[] nodes, String delimiter, String oldRulePath, String ruleStr) throws Exception
    {
        FileSystem fs = FileSystem.get(conf);

        if(ruleStr.equals("root"))
        {
            FSDataOutputStream fout = fs.create(new Path(conf.get(ArgumentsConstants.RULE_PATH)), true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
            bw.write("# [AttributeName-@@Attribute-Value][@@].., Data-Count, Node-Purity, Class-Label, Is-Leaf-Node" + "\n");
            for(int i=0; i<nodes.length; i++)
            {
            	String root = nodes[i].toString(delimiter);
            	System.out.println(root);
            	bw.write(root + "\n");
            }
            bw.close();
            fout.close();
        }
        else
        {
            FSDataInputStream fin = FileSystem.get(conf).open(new Path(oldRulePath));
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));

            FSDataOutputStream fout = fs.create(new Path(conf.get(ArgumentsConstants.RULE_PATH)), true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
            
            System.out.println("New Rule:" + ruleStr);//previous rule which has non leaf
            
            String readStr;
            while((readStr=br.readLine())!=null)
            {
                if(readStr.equals(ruleStr))
                {
                    if(nodes.length > 1)
                    {
                    	System.out.println(readStr+ "-cont");
                        bw.write(readStr + "-cont\n");
                        for(int i=0; i<nodes.length; i++)//Rule Append
                        {
                        	System.out.println(nodes[i].toString(delimiter));
                            bw.write(nodes[i].toString(delimiter) + "\n");
                        }
                    }
                    else
                    {
                		System.out.println(readStr + "-true");
                    	bw.write(readStr + "-true\n");
                    }
                }
                else 
                {
                	System.out.println(readStr);
                	bw.write(readStr + "\n");
                }
            }
            br.close();
            fin.close();
            bw.close();
            fout.close();
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
    		System.out.println("Less classes : " + Less_side.toString());
    		System.out.println("Bigger classes : " + Bigger_side.toString());
    		double L_Entropy = calculateShannonEntropy(Less_side);
    		double R_Entropy = calculateShannonEntropy(Bigger_side);
    		System.out.println("Less Entropy : " + L_Entropy);
    		System.out.println("Bigger Entropy : " + R_Entropy);
    	}
    	return point;
    }
    List<Double>  cutPoints = new ArrayList<Double>();//is null?
    
  //using minimum entropy
    private RuleNodeBaseInfo[] getSelectedNodes(Configuration conf) throws Exception
    {
        String delimiter = conf.get(ArgumentsConstants.DELIMITER);
        Path entropyPath = new Path(conf.get(ArgumentsConstants.OUTPUT_PATH));
        FileStatus[] status = FileSystem.get(conf).listStatus(entropyPath);

        String selectedStr = "";
        double minEntropy = 9999.0;
        double maxGainRatio = Double.MIN_VALUE;
        
        for(int i=0; i<status.length; i++)
        {
            if(!status[i].getPath().toString().contains("part-r-")) continue;

            FSDataInputStream fin = FileSystem.get(conf).open(status[i].getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));            
            String readStr;            
            while((readStr = br.readLine())!=null)
            {
            	
            	String[] x = readStr.split(delimiter);
            	String testbuffer = "";
            	for(int xi =2; xi < x.length; xi++)
            	{
            		testbuffer += x[xi] + ",";
            	}
            	testbuffer = testbuffer.substring(0, testbuffer.length()-1);
            	System.out.println("Column:" + x[0] + " Node Select: " + testbuffer);
                double curEntropy = Double.parseDouble(readStr.split(delimiter)[1]);                
                if(minEntropy > curEntropy)
                {
                    minEntropy = curEntropy;
                    selectedStr = readStr;
                }
            }           
            br.close();
            fin.close();
        }   
        String tokens[] = selectedStr.split(delimiter);
        System.out.println(Arrays.toString(tokens));
        int attrCnt = (tokens.length - 2) / 4;    // index, entropy and 4-set
        RuleNodeBaseInfo[] retNodes = new RuleNodeBaseInfo[attrCnt];
        int minDataCnt = Integer.parseInt(conf.get(ArgumentsConstants.MIN_LEAF_DATA));
        double minPurity = Double.parseDouble(conf.get(ArgumentsConstants.PURITY)); 
        for(int i=0; i<attrCnt; i++)
        {
            int base = i * 4;
            
            int dataCnt = Integer.parseInt(tokens[base+3]);
            double purity = Double.parseDouble(tokens[base+4]);
            String classlbl = tokens[base+5];
            String attrValue = tokens[base+2] ;
            String attrCondition = tokens[0];
           
            retNodes[i] = new RuleNodeBaseInfo(attrCondition, attrValue, dataCnt, purity , classlbl);    
            
            if((minDataCnt >= dataCnt) ||(minPurity < purity)) 
        	{     	
            	retNodes[i].setIsLeaf(true);   	
            }
            else 
        	{    
            	retNodes[i].setIsLeaf(false);   
            }        	
        }
        return retNodes;
    }
    double split_point = 0.0;
    double criteria = 0.0;
    double MDL_gain = 0.0;
    boolean leftsubset = false;
    int FTi = 0;
    
        
    public static boolean isStringDouble(String s)
    {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }    
}
