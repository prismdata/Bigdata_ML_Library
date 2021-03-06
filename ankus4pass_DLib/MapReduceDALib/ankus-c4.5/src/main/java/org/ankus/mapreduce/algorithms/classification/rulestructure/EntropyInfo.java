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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

/**
 * EntropyInfo
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.13
 * @author Moonie Song
 */
public class EntropyInfo {
	private Logger logger = LoggerFactory.getLogger(EntropyInfo.class);
    public double entropy = 1.0;
    public double GainRatio = 0.0; 
    double m_log2 = Math.log(2);

    public ArrayList<String> attrValueList = new ArrayList<String>();
    public ArrayList<Integer> attrValueTotalCntList = new ArrayList<Integer>();
    public ArrayList<Double> attrValuePurityList = new ArrayList<Double>();
    public ArrayList<String> attrMaxClassList = new ArrayList<String>();
    List<Double>  cutPoints = new ArrayList<Double>();
    public void setValueList(ArrayList<String> valueList)
    {
        attrValueList.addAll(valueList);
    }

    public void addAttributeDist(int sumArr[], int maxArr[], String classArr[])
    {
        int len = sumArr.length;

        for(int i=0; i<len; i++)
        {
            attrValueTotalCntList.add(sumArr[i]);
            attrValuePurityList.add((double)maxArr[i]/(double)sumArr[i]);
            attrMaxClassList.add(classArr[i]);
        }
    }

    public String toString(String delimiter)
    {
        String str = "" + entropy;
        int len = attrValueList.size();
        for(int i=0; i<len; i++)
        {
        	str += delimiter + attrValueList.get(i)
                    + delimiter + attrValueTotalCntList.get(i)
                    + delimiter + attrValuePurityList.get(i)
                    + delimiter + attrMaxClassList.get(i);

        }
        return str;
    }

    //Call From ID3ComputeEntropy
    //Compute Information Gain for one column
    public void computeIGVal(HashMap<String, HashMap<String, Integer>> attrClassList)
    {
        int attrValueTypeCount = attrClassList.size();
        System.out.println(attrClassList.toString());
        int sumArr[] = new int[attrValueTypeCount];
        double igArr[] = new double[attrValueTypeCount];
        int maxArr[] = new int[attrValueTypeCount];
        String classArr[] = new String[attrValueTypeCount];
        int totSum = 0;
        System.out.println("Size Of Attr Kind:" + attrValueTypeCount);
        for(int SelectedValueType=0; SelectedValueType<attrValueTypeCount; SelectedValueType++)
        {
            String valueStr = attrValueList.get(SelectedValueType);
            HashMap<String, Integer> classDistList = attrClassList.get(valueStr);

            int classCnt = classDistList.size();
            Iterator<String> classIter = classDistList.keySet().iterator();
            sumArr[SelectedValueType] = 0;
            int classArrInt[] = new int[classCnt];
            int idxClassValue = 0;
            logger.info("Class Distribution : " + classDistList.toString());
            while(classIter.hasNext())
            {
                String classVal = classIter.next().toString();
                classArrInt[idxClassValue] = classDistList.get(classVal);
                sumArr[SelectedValueType] += classArrInt[idxClassValue];
                logger.info("Class Instances:" + classArrInt[idxClassValue]);
                if(maxArr[SelectedValueType] < classArrInt[idxClassValue])
                {
                    maxArr[SelectedValueType] = classArrInt[idxClassValue];
                    classArr[SelectedValueType] = classVal;
                }
                idxClassValue++;
            }

            igArr[SelectedValueType] = getInformationValue(classArrInt, sumArr[SelectedValueType]);
            totSum += sumArr[SelectedValueType];
        }
        //하나의 컬럼과 해당 값들이 속하는 클래스 분포 정보 저장.
        addAttributeDist(sumArr, maxArr, classArr);
        logger.info("data count sum: " + Arrays.toString(sumArr));
        logger.info("max appears class: " + Arrays.toString(maxArr));
        logger.info("class list: " + Arrays.toString(classArr));
        entropy = getEntropy(sumArr, totSum, igArr);
    }
   
    public double conditionalEntropy(double rec_nums, List<Double>[] class_list)
    {
    	double rtn = 0.0;	
    	for(List<Double> class_dist: class_list)
    	{
    		if(class_dist.size() >0)
    		{
	    		double num_condi_class = 0.0;
	    		for(double classe :class_dist)
	    		{
	    			num_condi_class += classe;
	    		}
	    		double ent_i=0.0;
	    		
	    		for(double classe :class_dist)
	    		{
	    			double prob_xi= classe / num_condi_class;
	    			ent_i = ent_i + (prob_xi * (Math.log(prob_xi)/m_log2));
	    		}
	    		rtn += (num_condi_class / rec_nums) * -1 *ent_i;
    		}
    		
    	}
    	return rtn;
    }
    private double getC45Entropy(int[] classDist, int full_count)
    {
        double val = 0.0;
        
        for(int c: classDist)
        {
            double p = (double)c/(double)full_count;
            if(c > 0)
        	{
            	val = val + (p * Math.log(p)/m_log2);
        	}
        }
        if(val==0) 
        	return 0;
        else 
        	return val * -1;
       
    }
    
    private double getInformationValue(int[] classDist, int sum)
    {
        double val = 0.0;
        for(int c: classDist)
        {
            double p = (double)c/(double)sum;
            if(c > 0)
        	{
            	val = val + (p * Math.log(p)/m_log2);
        	}
        }
        if(val==0) return 0;
        else return val * -1;
    }

    private double getEntropy(int[] attrSumArr, int totalSum, double[] IGArr)
    {
        double val = 0.0;
        for(int i=0; i<attrSumArr.length; i++)
        {
        	System.out.println(">>>Labels Numbes:" + attrSumArr[i] + " Training Sz: " + (double)totalSum + " IG:" + IGArr[i]);
            val = val + ((double)attrSumArr[i] / (double)totalSum * IGArr[i]);
        }
        return val;
    }
    
    public String toStringGainRaio(String delimiter)
    {
        String str = entropy + delimiter;
        str += GainRatio;
        for(int i=0; i<attrValueList.size(); i++)
        {
            str += delimiter + attrValueList.get(i)
                    + delimiter + attrValueTotalCntList.get(i)
                    + delimiter + attrValuePurityList.get(i)
                    + delimiter + attrMaxClassList.get(i);
        }
        return str;
    }
    
    private double getSplitInfo(int[] numsofEachValues, int totalSum)
    {
        double val = 0.0;
        double dbltotalSum = 0;
        dbltotalSum = (double)totalSum;
        
        for(int i=0; i<numsofEachValues.length; i++)
        {
        	double dblAttrSumArr = 0;
        	dblAttrSumArr = (double)numsofEachValues[i];
        	
        	double proportion = (dblAttrSumArr/ dbltotalSum);
        	
            val += -1 * proportion * (Math.log(proportion)/m_log2);
        }
        return val;
    }
    double split_point = 0.0;
    double criteria = 0.0;
    double MDL_gain = 0.0;
    boolean leftsubset = false;
    int FTi = 0;
    
    /*
    HashMap<Double, Double> GainMap = new HashMap<Double, Double>();
    private List<Double> cutPointsForSubset(String[] args, double new_start, double new_end, double new_max, Configuration conf)
    {
    	
    	List<Double>  left  = new ArrayList<Double>();
    	List<Double> right = new ArrayList<Double>();
    	double bestCutPoint = -1;
    	
    	getSV(args, new_start, new_end, conf);
    	bestCutPoint = this.split_point ;
    	if(this.MDL_gain <= 0)
    	{
    		return null;
    	}
    	//Fayyad - Iranis MDL Criteria compare...
    	if(this.MDL_gain > this.criteria)
    	{
    		leftsubset = true;
    		new_end = this.split_point;
    		left = cutPointsForSubset(args, new_start, new_end, new_max,conf);   		
    		
    		leftsubset = false;
    		new_start = new_end;
    		new_end = new_max;
    		right = cutPointsForSubset(args, new_start, new_end, new_max, conf);
    		
    		if ((left == null) && (right == null))
    		{
    			if(cutPoints.contains(bestCutPoint) == false)
    			{
    				cutPoints.add(bestCutPoint);
    				GainMap.put(this.MDL_gain, bestCutPoint);
    			}
    		} 
    		else if (right == null)
    		{
    			if(cutPoints.contains(bestCutPoint) == false)
    			{
    				cutPoints.add(bestCutPoint);
    				GainMap.put(this.MDL_gain, bestCutPoint);
    			}
    		}
    		else if (left == null)
    		{
    			if(cutPoints.contains(bestCutPoint) == false)
    			{
    				cutPoints.add(bestCutPoint);
    				GainMap.put(this.MDL_gain, bestCutPoint);
    			}
    		} 
    		else
    		{
    			if(cutPoints.contains(bestCutPoint) == false)
    			{
    				cutPoints.add(bestCutPoint);
    				GainMap.put(this.MDL_gain, bestCutPoint);
    			}
    		}
    		return cutPoints;
    	}
    	else
    	{
    		return null;
    	}
    }
    */
    public static boolean isStringDouble(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
      }
    /*
    @SuppressWarnings({ "deprecation", "deprecation" })
	public int getSV(String[] args, double new_start, double new_end,  Configuration conf)
    {
        boolean BTemp = false;
        boolean split_stop = false;
        int split_count = 0;
    	try
    	{
    		
	        List<Double> split_List = new ArrayList<Double>();
	        
 		  	conf.setBoolean("BTemp", false); 		  	
 		  	conf.setDouble("new_start", new_start);
 		  	conf.setDouble("new_end", new_end);
 		  	conf.setBoolean("leftsubset", leftsubset);
 		  	conf.setInt("FilterTarget", 0);
 	        Job jobPrepare = new Job(conf);
 	        FileSystem.get(conf).delete(new Path(conf.get("MDL_OUTPUT")), true);
 	        System.out.println("MDL:INPUT:" + conf.get("MDL_INPUT"));
 	        
 	        FileInputFormat.setInputPaths(jobPrepare, new Path(conf.get("MDL_INPUT"))); //input : sorted result
 	       
 	        FileOutputFormat.setOutputPath(jobPrepare, new Path(conf.get("MDL_OUTPUT")));
 	        //jobPrepare.setJarByClass(EntropyInfo.class);
 	        jobPrepare.setMapperClass(EntropyDiscPrepareMapper.class);
 	        jobPrepare.setReducerClass(EntropyDisPrepareReducer.class);
 	        jobPrepare.setOutputKeyClass(Text.class);
 	        jobPrepare.setOutputValueClass(Text.class);
 	        MultipleOutputs.addNamedOutput(jobPrepare, "sortedattribute", TextOutputFormat.class, Text.class, Text.class);
 	        if(!jobPrepare.waitForCompletion(true))
 	        {
 	            //logger.info("Error: jobPrepare is not Completeion");
 	            return 1;
 	        }   
 	       //logger.info("jobPrepare is  Completeion");
         
 	        @SuppressWarnings("deprecation")
 	        
			Job job_getEntropy = new Job(conf);
 	        FileSystem.get(conf).delete(new Path(conf.get("MDL_OUTPUT") +"_entropy"));//FOR LOCAL TEST
 	        //INPUT PATH
 	        //System.out.println("job_getEntropy:INPUT" + conf.get("MDL_OUTPUT")+"/sortedattribute-r-00000");
 	        FileInputFormat.setInputPaths(job_getEntropy, conf.get("MDL_OUTPUT")+"/sortedattribute-r-00000");
 	        //OUTPUT PATH
 	        //System.out.println("job_getEntropy:OUTPUT" + conf.get("MDL_OUTPUT")+"/_entropy");
 	        FileSystem.get(conf).delete(new Path(conf.get("MDL_OUTPUT")+"_entropy"), true);//FOR LOCAL TEST
 	        FileOutputFormat.setOutputPath(job_getEntropy, new Path(conf.get("MDL_OUTPUT")+"_entropy"));
 	        
 	        job_getEntropy.setJarByClass(EntropyInfo.class);
 	        job_getEntropy.setMapperClass(EntropyDisSelAttributeMapper.class);
 	        job_getEntropy.setReducerClass(EntropyDisSelAttributeReducer.class);
 	        job_getEntropy.setOutputKeyClass(Text.class);
 	        job_getEntropy.setOutputValueClass(Text.class);
 	        MultipleOutputs.addNamedOutput(job_getEntropy, "entropy", TextOutputFormat.class,	Text.class, Text.class);
 	        if(!job_getEntropy.waitForCompletion(true))
 	        {
 	        	 //logger.info("Error:job_getEntropy is not Completeion");
 	             return 1;
 	        }
 	        //logger.info("job_getEntropy is Completeion");
 	        @SuppressWarnings("deprecation")
			Job get_weightedEntrpy = new Job(conf);
 	       
 	        FileInputFormat.setInputPaths(get_weightedEntrpy, conf.get("MDL_OUTPUT")+"_entropy");
 	        
 	        FileSystem.get(conf).delete(new Path(conf.get("MDL_OUTPUT") +"_weightedentropy"),true);//FOR LOCAL TEST
 	        FileOutputFormat.setOutputPath(get_weightedEntrpy, new Path(conf.get("MDL_OUTPUT")+"_weightedentropy"));
 	        get_weightedEntrpy.setJarByClass(EntropyInfo.class);
 	        get_weightedEntrpy.setMapperClass(EntropyWeightedMapper.class);
 	        get_weightedEntrpy.setReducerClass(EntropyWeightedReducer.class);
 	        get_weightedEntrpy.setOutputKeyClass(Text.class);
 	        get_weightedEntrpy.setOutputValueClass(Text.class);
 	        MultipleOutputs.addNamedOutput(get_weightedEntrpy, "weightedentropy", TextOutputFormat.class,	Text.class, Text.class);
 	        if(!get_weightedEntrpy.waitForCompletion(true))
 	        {
 	        	// logger.info("Error: get_weightedEntrpy is not Completeion");
 	             return 1;
 	        }
 	       //logger.info("get_weightedEntrpy is Completeion");
 	      
 	        Double min_entropy = Double.MAX_VALUE;
 			Path path = null;
 			BufferedReader br = null;
 			String line = "";
 			List<Double> attribute_temp = new ArrayList<Double>();
 			double[] sp = new double[2];
 	        try
 			{
 	        	//Minimum Entropy find -> SplitPoint candidate
 				String[] token = null;
 				path = new Path(conf.get("MDL_OUTPUT")+"_weightedentropy/weightedentropy-r-00000");
 				FileSystem fs = FileSystem.get(conf);
 				br = new BufferedReader(new InputStreamReader(fs.open(path)));
 					
 				int spi = 0;
 				while((line = br.readLine())!= null)
 				{
 					token = line.split("\t");
 					double attribute = Double.parseDouble(token[0]);
 					double entropy = Double.parseDouble(token[1]);
 					//System.out.println("Current Attribute: " + attribute + " Weighted Current entropy:" + entropy);
 					attribute_temp.add(attribute);
 					
					if(min_entropy > entropy)
 					{
 						min_entropy = entropy;
 						split_point = attribute;
 					}
 				}
 				br.close();
 			}
 	        catch(Exception e)
 			{
 				//logger.info(e.toString());
 			}
 	        //System.out.println("Best Cut : " + split_point + " Min entropy:" + min_entropy);	 	        		
    		conf.setDouble("min_entropy", min_entropy);
 			conf.setDouble("split_point", split_point);
 			
 			conf.setDouble("new_start", new_start);
 		  	conf.setDouble("new_end", new_end);
 		  	conf.setBoolean("leftsubset", leftsubset);
 		  	conf.setInt("FilterTarget", FTi);
 			Job jobGain_Criteria = new Job(conf);
 			
 			FileInputFormat.setInputPaths(jobGain_Criteria, conf.get("MDL_INPUT"));
 			
 			FileSystem.get(conf).delete(new Path(conf.get("MDL_OUTPUT") +"_Evaluation"), true);//FOR LOCAL TEST
 			FileOutputFormat.setOutputPath(jobGain_Criteria, new Path(conf.get("MDL_OUTPUT")+"_Evaluation"));
 	        jobGain_Criteria.setJarByClass(EntropyInfo.class);
 	        jobGain_Criteria.setMapperClass(EntropyEvaluMapper.class);
 	        jobGain_Criteria.setReducerClass(EntropyEvaluReducer.class);
 	        jobGain_Criteria.setOutputKeyClass(Text.class);
 	        jobGain_Criteria.setOutputValueClass(Text.class);
 	        if(!jobGain_Criteria.waitForCompletion(true))
 	        {
 	        	 
 	             return 1;
 	        }
 	       
 	       
 	        Counters counters = jobGain_Criteria.getCounters();
 	        Counter Counter_gain = counters.findCounter(Entropy_result.gain);	        
 	     	//System.out.println(Counter_gain.getDisplayName()+":"+Counter_gain.getValue());
 	     	
 	     	Counter Counter_criteria = counters.findCounter(Entropy_result.criteria);	        
 	     	//System.out.println(Counter_criteria.getDisplayName()+":"+Counter_criteria.getValue());
 	     	
 	     	this.MDL_gain = Double.parseDouble(Counter_gain.getDisplayName().replace("gain:", "")); 
 	     	this.criteria = Double.parseDouble(Counter_criteria.getDisplayName().replace("criteria:",""));
 	     }
    	catch(Exception e)
    	{
    		System.out.println(e.toString());
    		
    		return -1;
    	}
		return 0; 	        
    }
    */
}
