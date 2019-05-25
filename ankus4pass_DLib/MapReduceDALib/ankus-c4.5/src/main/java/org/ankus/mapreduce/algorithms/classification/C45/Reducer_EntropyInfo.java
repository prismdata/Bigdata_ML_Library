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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
/*
import org.ankus.mapreduce.algorithms.classification.C45MDL.C45MDL;
import org.ankus.mapreduce.algorithms.classification.C45MDL.EntropyDisPrepareReducer;
import org.ankus.mapreduce.algorithms.classification.C45MDL.EntropyDisSelAttributeMapper;
import org.ankus.mapreduce.algorithms.classification.C45MDL.EntropyDisSelAttributeReducer;
import org.ankus.mapreduce.algorithms.classification.C45MDL.EntropyDiscPrepareMapper;
import org.ankus.mapreduce.algorithms.classification.C45MDL.EntropyEvaluMapper;
import org.ankus.mapreduce.algorithms.classification.C45MDL.EntropyEvaluReducer;
import org.ankus.mapreduce.algorithms.classification.C45MDL.EntropyWeightedMapper;
import org.ankus.mapreduce.algorithms.classification.C45MDL.EntropyWeightedReducer;
import org.ankus.mapreduce.algorithms.classification.C45MDL.EntropyEvaluReducer.Entropy_result;
*/
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
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
 * @date : 2016.04.04
 * @author SHINHONGJOONG
 */

public class Reducer_EntropyInfo {

    public double entropy = 1.0;
    public double GainRatio = 0.0; 
    public double InfoGain = 0.0; 
    double m_log2 = Math.log(2);

    public ArrayList<String> attrValueList = new ArrayList<String>();
    public ArrayList<Integer> attrValueTotalCntList = new ArrayList<Integer>();
    public ArrayList<Double> attrValuePurityList = new ArrayList<Double>();
    public ArrayList<String> attrMaxClassList = new ArrayList<String>();
    List<Double>  cutPoints = new ArrayList<Double>();
    
    Distribution distributionClass, distributionClass_ReArrange;
    public String Variable_SplitValue =  "";
    private Logger logger = LoggerFactory.getLogger(Reducer_EntropyInfo.class);
    static final HashMap<String, Double> key_gain_map = new HashMap<String, Double>();		
    static final HashMap<String, Double> key_gainratio_map = new HashMap<String, Double>();		
    public void setValueList(ArrayList<String> valueList)
    {
        attrValueList.addAll(valueList);
    }
    
    //attribute idx, gain, {a split_value, class distribution for (T1,...,Tn)}
    //splitValue-T1:Class1:N,Class2:N..,ClassN:M
    public void setAttributeDist(double param_InfoGain, double param_GainRatio, String param_SplitInfo)
    {
    	DecimalFormat format = new DecimalFormat(".#############");
    	this.InfoGain = Double.parseDouble(format.format(param_InfoGain));
    	
    	this.GainRatio = Double.parseDouble(format.format(param_GainRatio));
    	
    	this.Variable_SplitValue = param_SplitInfo;
    }
    public String putAttributeDist()
    {	
    	if(InfoGain == 0)
    	{
    		return null;
    	}
    	return InfoGain + "\u0000" + GainRatio + "\u0001" + Variable_SplitValue;
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
        	/*
            str += delimiter + attrValueList.get(i)
                    + delimiter + attrValueTotalCntList.get(i)
                    + delimiter + attrValuePurityList.get(i)
                    + delimiter + attrMaxClassList.get(i);
            */
            str += delimiter + attrValueList.get(i)
                    + delimiter + "999"
                    + delimiter + "888"
                    + delimiter + attrMaxClassList.get(i);
        }
        return str;
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
    public double nLog(double num)
    {
        if (num <= 0)
        {
          return 0;
        }
        else
        {
          return num * Math.log(num);
        }
    }
    private  double newEnt(Distribution bags) {
        
        double returnValue = 0;
        int SubTree_idx, class_idx;

        for (SubTree_idx=0;SubTree_idx<bags.numBags();SubTree_idx++)
        {
          for ( class_idx=0; class_idx<bags.numClasses(); class_idx++)
          {
        	  double ClassCount = bags.perClassPerBag(SubTree_idx, class_idx);
        	  returnValue = returnValue+nLog(ClassCount);
          }  
          double numbersOfInstance = bags.perBag(SubTree_idx);
          returnValue = returnValue-nLog(numbersOfInstance);
        }
        return -(returnValue/Math.log(2));
      }
    private double splitCritValue(Distribution bags, double totalNoInst, double oldEnt) {

        double numerator;
        double noUnknown;
        double unknownRate;
        noUnknown = totalNoInst - bags.total();
        unknownRate = noUnknown / totalNoInst;
        numerator = (oldEnt - newEnt(bags));
        numerator = (1 - unknownRate) * numerator;

        // Splits with no gain are useless.
        if (numerator == 0) 
        {
          return 0;
        }

        return numerator / bags.total();
    }
    private double gainRatioCrit_splitCritValue(Distribution bags, double totalnoInst,   double numerator) 
    {
    	    double SplitInfo;
    	    // Compute split info.    	    
    	    SplitInfo = splitEnt(bags, totalnoInst);
    	    // Test if split is trivial.
    	    if (SplitInfo == 0) {
    	      return 0;
    	    }
    	    SplitInfo = SplitInfo / totalnoInst;
    	    double gainratio  = numerator / SplitInfo;  	       
    	    return numerator / SplitInfo;
    }
    private double splitEnt(Distribution bags, double totalnoInst)
    {
    	double returnValue = 0;
        double noUnknown;
        int i;
        noUnknown = totalnoInst - bags.total();
        if (bags.total() > 0)
		{
          for (i = 0; i < bags.numBags(); i++) {
            returnValue = returnValue - nLog(bags.perBag(i));
          }
          returnValue = returnValue - nLog(noUnknown);
          returnValue = returnValue + nLog(totalnoInst);
        }
        return returnValue / Math.log(2);
      }
    
	public void computeGainRatio(Configuration conf, String key, HashMap<String, HashMap<String, Integer>> ValueClassList_ASC)
    {
		try
        {
		    int m_indexArr[];
		    int m_numericIndexArr[];
		    m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.NOMINAL_INDEX, "-1"));
	        m_numericIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
	        
			double max_gainRatio = 0.0;
			String current_idx = key;
			double defaultEnt = 0.0;	
	    	boolean isRoot = true;    	
	    	int attrSize = ValueClassList_ASC.size();
	    	
//	    	String[] args = null;
//	    	String MDL_Input = conf.get(ArgumentsConstants.OUTPUT_PATH);
	    	double rec_count = 0.0;
	    	
	    	int sum[] = new int[attrSize];//NUMS OF DATA
	        int maxArr[] = new int[attrSize]; //Nums of Purity
	        String classArr[] = new String[attrSize]; //Class label
	        
	        Iterator<String> unique_value  = ValueClassList_ASC.keySet().iterator();
	        int data_idx = 0;
	        while(unique_value.hasNext())
	        {
				String attributeValue = unique_value.next();
				int count = 0;
				HashMap<String, Integer> MashClass_Count = ValueClassList_ASC.get(attributeValue);
				Iterator<String> ItrCC  = MashClass_Count.keySet().iterator();				
				int largest_Count = -1 * Integer.MAX_VALUE;
				String decision_Label = "";
				while(ItrCC.hasNext())
		        {
					String ClassLabel = ItrCC.next();
					int tmpCnt = MashClass_Count.get(ClassLabel);
					if(largest_Count < tmpCnt)
					{
						largest_Count = tmpCnt;
						decision_Label = ClassLabel;
					}
					count += tmpCnt;					
		        }
				data_idx++;
	        }
        
	        //전체 엔트로피 계산 시작.
	        HashMap<String, Integer> ClassDistribution =  new HashMap<String, Integer>();
	        Iterator<String> attribute  = ValueClassList_ASC.keySet().iterator();
	        while(attribute.hasNext())
	        {
	        	String attributeValue = attribute.next();
	        	Iterator<String> attribute2  = ValueClassList_ASC.get(attributeValue).keySet().iterator();
	        	
	        	while(attribute2.hasNext())
	            {
	        		String classLabel = attribute2.next().toString();        		 
					int labl_cnt = ValueClassList_ASC.get(attributeValue).get(classLabel);
	 
					if(ClassDistribution.containsKey(classLabel) == true)
					{
	        			 int tmp_cnt = ClassDistribution.get(classLabel);
	        			 ClassDistribution.put(classLabel, tmp_cnt+labl_cnt);
					}
	        		else
	        		{
	        			ClassDistribution.put(classLabel, labl_cnt);
	        		}
	             }
	        }
	        //Get all instnaces
	        Iterator<String> Itr_forClassCount = ClassDistribution.keySet().iterator();
	        while(Itr_forClassCount.hasNext())
			{
				String classLabel = Itr_forClassCount.next().toString();        	
				int label_cnt = ClassDistribution.get(classLabel); 
				rec_count += (double)label_cnt;
			}
	        int KindOfClass  = ClassDistribution.size();
        	defaultEnt = 0;
			Iterator<String> ClassDistItr = ClassDistribution.keySet().iterator();
			double logSum = 0.0;			
			
			while(ClassDistItr.hasNext())
			{
				String classLabel = ClassDistItr.next().toString();        	
				double num = ClassDistribution.get(classLabel);
		    	double lnValue = nLog(num);
		    	logSum = logSum + lnValue;
			}
		    double total_ln = nLog(rec_count);
		    defaultEnt =  (total_ln - logSum)/Math.log(2);
	        String sp_class_data = "";
	        
	        HashMap<String, Double> subT1ClassDist = new HashMap<String, Double>();
	        HashMap<String, Double> subT2ClassDist = new HashMap<String, Double>();	
	        HashMap<String, Double> subT1ClassDist_MaxGain = new HashMap<String, Double>();
	        HashMap<String, Double> subT2ClassDist_MaxGain = new HashMap<String, Double>();	
	        String tmp_sp_class_data = "";
	        
	        Collections.sort(attrValueList, new Comparator<String>(){public int compare(String obj1, String obj2)
																							{return obj1.compareToIgnoreCase(obj2); }});
	        
	        String FirstValue = attrValueList.get(0);
	        
	        if(FirstValue.contains("@@") == true)
	        {
	        	String[] tmp  = FirstValue.split("@@");
	        	String sv = tmp[tmp.length-1];
	        	FirstValue = sv;
	        	isRoot = false;
	        }
	        int ColumnIdx = Integer.parseInt(key);
//	        boolean isNumber  = false;
	        int isNumber = 0;
	        if(m_indexArr[0] != -1 && CommonMethods.isContainIndex(m_indexArr, ColumnIdx , true))
	        {
	        	 isNumber  = 0;
	        }
	        else if(CommonMethods.isContainIndex(m_numericIndexArr, ColumnIdx , true))
	        {
	        	isNumber  = 1;
	        }
	        else
	        {
	        	logger.info("Unknown");
	        }
//	        if(isNumber  == false)
	        switch(isNumber)
	        {
	        case 0:
	        {
	        	double Norminal_split_info = 0.0;
	        	double NorminalGain = 0;
	        	double classCount = 0.0;
	        	double attributeEntropy = 0.0;
	        	double Entropy =  0.0;
	        	try
	        	{
	        	Itr_forClassCount = ValueClassList_ASC.keySet().iterator();
	        	
	        	//Routine to get total count
	            while(Itr_forClassCount.hasNext())
	    		{
	    			String splitValue = Itr_forClassCount.next().toString();
		        	HashMap<String, Integer> distribution  = ValueClassList_ASC.get(splitValue);
		        	Iterator<String> ClassLabel = distribution.keySet().iterator();
		        	while(ClassLabel.hasNext())
					{
						String classVal = ClassLabel.next().toString();
						classCount += distribution.get(classVal);
					}
	    		}
	            
	            
	            tmp_sp_class_data += "C"+"\u0007";	            	           
	        	Itr_forClassCount = ValueClassList_ASC.keySet().iterator();
	            while(Itr_forClassCount.hasNext())
	    		{
	    			String splitValue = Itr_forClassCount.next().toString();
	    			
		        	HashMap<String, Integer> distribution  = ValueClassList_ASC.get(splitValue);
		        	tmp_sp_class_data += "\u0003" +  splitValue + "##";
		        	Iterator<String> ClassLabel = distribution.keySet().iterator();
		        	double[] each_Count = new double[distribution.size()];
		        	int class_idx = 0;
		        	double subclassCount = 0.0;
		        	while(ClassLabel.hasNext())
					{
						String classVal = ClassLabel.next().toString();
						subclassCount += distribution.get(classVal);
						each_Count[class_idx] = distribution.get(classVal);
						tmp_sp_class_data += classVal +"::" +  distribution.get(classVal) + "\u0002";
						class_idx++;
					}
		        	tmp_sp_class_data = tmp_sp_class_data.substring(0, tmp_sp_class_data.length()-1);
		        	Entropy = 0.0;
		        	for(int i = 0; i < each_Count.length; i++)
		        	{
		        		double prob = each_Count[i]/subclassCount;
		        		Entropy -= prob * (Math.log(prob) / Math.log(2));
		        	}
//			        	double siRatio = subclassCount / classCount;
		        	attributeEntropy += (subclassCount / classCount) * Entropy;
	    		}
		            NorminalGain = defaultEnt - attributeEntropy;
		            GainRatio = NorminalGain;
		            addAttributeDist(sum, maxArr, classArr);
//		            logger.info("data count sum: " + Arrays.toString(sum));
//		            logger.info("max appears class: " + Arrays.toString(maxArr));
//		            logger.info("class list: " + Arrays.toString(classArr));
	            setAttributeDist(NorminalGain, GainRatio,tmp_sp_class_data);
	        	}
	        	catch(Exception e)
	        	{
	        		logger.error(e.toString());
	        	}
	        }
	        break;
			//수치형 데이터 분포
	        case 1:
		    {
		        //수치형 데이터 분포 생성.
//	        	double averageInfoGain = 0;
		        double Numeric_InfoGain = 0.0;
//		        double Numeric_Split_info = 0.0;
		        double Numeric_max_gain = -1 * Double.MAX_VALUE;
	        	List<String> classList = new ArrayList<String>();
	        	double T1Entropy = 0.0, T2Entropy = 0.0;
	        	double splitValue  = 0.0;
	        	//분할 지점 후보 조회 시작.
	        	int next = 0;
	        	int splitIndex = -1;
		        for(; next < attrSize; next++)
		        {
		        	double NumbersOf_Less = 0.0, NumbersOf_More = 0.0;
		        	String feature_value  = "";
		        	subT1ClassDist.clear();
		            subT2ClassDist.clear();  
		        	if(isRoot == false)
		            {
		            	String[] tmp  = attrValueList.get(next).split("@@");
		            	String sv = tmp[tmp.length-1];
		            	splitValue = Double.parseDouble(sv);
		            }
		        	else
		        	{
		        		splitValue  = Double.parseDouble(attrValueList.get(next));
		        	}
		        	
			        for(int i = 0 ; i< attrSize; i++)
					{
			        	feature_value = attrValueList.get(i);
						double dblFeatureValue = 0.0;
						if(isRoot == false)
			            {
			            	String[] tmp  = feature_value.split("@@");
			            	dblFeatureValue = Double.parseDouble(tmp[tmp.length-1]);
			            }
						else
						{
							dblFeatureValue = Double.parseDouble(feature_value);//temporary split value...
						}		
						
			        	if(splitValue + 1e-5  <= dblFeatureValue)
						{
			        		////System.out.println(splitValue + "~"+ dblFeatureValue);
			        		Iterator<String> classIter = ValueClassList_ASC.get(feature_value).keySet().iterator();
							while(classIter.hasNext())
							{
								String classVal = classIter.next().toString();
								double cntSub1 = 0.0;
								if(classList.contains(classVal) == false)
								{classList.add(classVal); }
								NumbersOf_Less += (double)ValueClassList_ASC.get(feature_value).get(classVal); 
								if(subT1ClassDist.containsKey(classVal))
								{
									cntSub1 = subT1ClassDist.get(classVal) + ValueClassList_ASC.get(feature_value).get(classVal);
									subT1ClassDist.put(classVal, cntSub1);
								}
								else
								{
									cntSub1 = ValueClassList_ASC.get(feature_value).get(classVal);
									subT1ClassDist.put(classVal, cntSub1);
								}
							}
						}
						else
						{
							//splitValue이상을 갖는 부분집합의 클래스 분포획득.
//							//System.out.println(splitValue + "~"+ dblFeatureValue);
							Iterator<String> classIter = ValueClassList_ASC.get(feature_value).keySet().iterator();
							while(classIter.hasNext())
							{
								String classVal = classIter.next().toString();
								double cntSub2 = 0.0;
								if(classList.contains(classVal) == false)
								{classList.add(classVal); }
								NumbersOf_More += (double)ValueClassList_ASC.get(feature_value).get(classVal); 
								if(subT2ClassDist.containsKey(classVal))
								{
									cntSub2 = subT2ClassDist.get(classVal) + ValueClassList_ASC.get(feature_value).get(classVal);
									subT2ClassDist.put(classVal, cntSub2);
								}
								else
								{
									cntSub2 = ValueClassList_ASC.get(feature_value).get(classVal);
									subT2ClassDist.put(classVal, cntSub2);
								}
							}
						}
					}
			        
//			        System.out.println("Decision1: "+splitValue);
			        //******비교 대상 점에서 클래스 분포 수집 종료.
			        //****엔트로피 계산용 클래스 호출.
			        distributionClass = new Distribution(2, KindOfClass);
			        ClassDistItr = ClassDistribution.keySet().iterator();
			        int ClassIdx = 0;
			        while(ClassDistItr.hasNext())
			        {
			        	String classLabel = ClassDistItr.next().toString();        	
			        	double num = ClassDistribution.get(classLabel);
			        	distributionClass.m_perClass[ClassIdx] = num;
			        	ClassIdx++;
			        }
			        distributionClass.m_perBag[0] = NumbersOf_Less;
			        distributionClass.m_perBag[1] = NumbersOf_More;
			        distributionClass.totaL = NumbersOf_Less + NumbersOf_More;
			        double	minSplit = 0;
//			        minSplit = 0.1 * (distributionClass.totaL) / (ClassIdx);			        
			        int m_minNoObj = conf.getInt("-minLeafData", 0);			        
//        		    if (minSplit <= m_minNoObj)
//					{
//        		    	minSplit = m_minNoObj;
//        		    }
//        		    if ((NumbersOf_Less >= minSplit) && (NumbersOf_More >= minSplit))
			        
//			        System.out.println("************ Left:" + NumbersOf_Less + " Right:" + NumbersOf_More + " Blance : " + (NumbersOf_Less - NumbersOf_More));
			        if (true) 
        	        {
 				        //클래스 분포를 통한 엔트로피를 구하기 위해 신규 생성된 정보 수집.
				        int SubTreeCls_Cnt = 0;
				        Iterator<String> SubTreeDistItr = subT1ClassDist.keySet().iterator();
				        while(SubTreeDistItr.hasNext())
				        {
							String classLabel = SubTreeDistItr.next().toString();        	
							distributionClass.m_perClassPerBag[0][SubTreeCls_Cnt] = subT1ClassDist.get(classLabel);
							SubTreeCls_Cnt++;
				        }
				        SubTreeDistItr = subT2ClassDist.keySet().iterator();
				        SubTreeCls_Cnt = 0;
				        while(SubTreeDistItr.hasNext())
				        {
							String classLabel = SubTreeDistItr.next().toString();        	
							distributionClass.m_perClassPerBag[1][SubTreeCls_Cnt] = subT2ClassDist.get(classLabel);
							SubTreeCls_Cnt++;
				        }
				        
				        //*****정보 수집 완료.
				        Numeric_InfoGain =  this.splitCritValue(distributionClass, distributionClass.totaL, defaultEnt);
				        //**gain이 최대인 것에 대한 클래스분포 업데이트. 
				        if(Numeric_max_gain  < Numeric_InfoGain)
				        {
				        	subT1ClassDist_MaxGain = (HashMap<String, Double>) subT1ClassDist.clone();
				        	subT2ClassDist_MaxGain = (HashMap<String, Double>) subT2ClassDist.clone();
				        	distributionClass_ReArrange = new Distribution(2, KindOfClass);
				        	distributionClass_ReArrange = (Distribution)distributionClass.clone();
				        	splitIndex = next;
				        	Numeric_max_gain  =  Numeric_InfoGain;
				        	max_gainRatio = gainRatioCrit_splitCritValue(distributionClass_ReArrange, distributionClass.totaL, Numeric_max_gain);
				        }
        	        }
		        }
		        
//		        //System.out.println("KEY IDX:" + current_idx + " Max Gain/Ratio: " + Numeric_max_gain +"/"+ max_gainRatio);
		        double splitPoint = 0.0;
		        if(isRoot == false)
	            {
	            	String[] tmp  = attrValueList.get(splitIndex).split("@@");
	            	String sv = tmp[tmp.length-1];
	            	splitValue = Double.parseDouble(sv);
	            }
	        	else
	        	{
//	        		//System.out.println(attrValueList.get(splitIndex +1)  + "~" +  attrValueList.get(splitIndex));
	        		splitValue = (Double.parseDouble(attrValueList.get(splitIndex +1)) + Double.parseDouble(attrValueList.get(splitIndex)))/2;
//	        		//System.out.println("Middle "+ splitValue);	        		 
	        		if (splitValue == Double.parseDouble(attrValueList.get(splitIndex +1)))
			        {
			        	splitValue = Double.parseDouble(attrValueList.get(splitIndex));
			        }
	        		
	        	}
		        key_gain_map.put(current_idx, Numeric_max_gain);
		        key_gainratio_map.put(current_idx, max_gainRatio);
		        if(key_gain_map.size() == 4)
		        {
		        	key_gain_map.clear();
		        	key_gainratio_map.clear();
		        }
		        
		        //System.out.println(">>>>Total Count : " + distributionClass.totaL);
		        //System.out.println(">>>>GainRatio : " + max_gainRatio);
		        //System.out.println(">>>>Numeric_InfoGain : " + Numeric_max_gain);
		        //System.out.println(">>!!!!SplitValue : " + splitValue);
		        
		        //System.out.println("****ValueClassList : "  + ValueClassList_ASC.toString());
		        //System.out.println("****Tree T1 Class Distribution : " + subT1ClassDist.toString());
		        //System.out.println("****Tree T2 Class Distribution : " + subT2ClassDist.toString());
		        //System.out.println("****Tree Entropy : " + defaultEnt);

		        if(subT1ClassDist_MaxGain.size() > 0)
		        {
		        	Iterator<String> mapClasses = subT1ClassDist_MaxGain.keySet().iterator();
			        tmp_sp_class_data += "##>" + "\u0003";
			        while( mapClasses.hasNext())
			        {
			            String mapClasse = mapClasses.next();
			            int count =  Integer.parseInt(String.valueOf(Math.round(subT1ClassDist_MaxGain.get(mapClasse))));
			            tmp_sp_class_data += mapClasse +"::" +  count + "\u0002";
			        }
			        tmp_sp_class_data = tmp_sp_class_data.substring(0, tmp_sp_class_data.length()-1);
		        }
		        if(subT2ClassDist_MaxGain.size() > 0)
		        {
		        	Iterator<String> mapClasses = subT2ClassDist_MaxGain.keySet().iterator();
			        tmp_sp_class_data += "##<" + "\u0003";
			        while( mapClasses.hasNext())
			        {
			            String mapClasse = mapClasses.next();
			            int count =  Integer.parseInt(String.valueOf(Math.round(subT2ClassDist_MaxGain.get(mapClasse))));
			            tmp_sp_class_data += mapClasse +"::" +  count + "\u0002";
			        }
			        tmp_sp_class_data = tmp_sp_class_data.substring(0, tmp_sp_class_data.length()-1);
		        }
		        //분할 지점 후보 조회 종료.
		        String fValues = "";
		        if(isRoot == false)
	            {
		        	for (String fValuesTemp: ValueClassList_ASC.keySet())
	        		{
		            	String[] tmp  = fValuesTemp.split("@@");
		            	String sv = tmp[tmp.length-1];
		            	fValues += sv + "\u0005";
	        		}
	            }
	        	else
	        	{
	        		for (String fValuesTemp: ValueClassList_ASC.keySet())
	        		{
	        			fValues += fValuesTemp + "\u0005";
	        		}
	        	}
		        fValues  = fValues.substring(0, fValues.length()-1);
		        
		        sp_class_data = "N" +"\u0007" + splitValue + tmp_sp_class_data + "\u0004"+ fValues;
		        if(Numeric_max_gain == 0)
		        {
		        	System.out.println("check");
		        }
		        addAttributeDist(sum, maxArr, classArr);
//	            logger.info("data count sum: " + Arrays.toString(sum));
//	            logger.info("max appears class: " + Arrays.toString(maxArr));
//	            logger.info("class list: " + Arrays.toString(classArr));
		        setAttributeDist(Numeric_max_gain, max_gainRatio , sp_class_data);
	        }//end of all values search.
	        }
        }
		catch(Exception e)
		{
			//System.out.println(e.toString());
		}
    }    

	public static List sortByValue(final Map map){
        List<Double> list = new ArrayList();
        list.addAll(map.keySet());
         
        Collections.sort(list,new Comparator(){
             
            public int compare(Object o1,Object o2){
                Object v1 = map.get(o1);
                Object v2 = map.get(o2);
                 
                return ((Comparable) v1).compareTo(v2);
            }
             
        });
        Collections.reverse(list); // 주석시 오름차순
        return list;
    }
    public double conditionalEntropy(double rec_nums, List<Double>[] class_list)
    {
    	double rtn = 0.0;	
    	for(List<Double> class_dist: class_list)
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
    			ent_i += prob_xi * (Math.log(prob_xi)/m_log2);
    		}
    		rtn += (num_condi_class / rec_nums) *ent_i;
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
        	//System.out.println(">>>Labels Numbes:" + attrSumArr[i] + " Training Sz: " + (double)totalSum + " IG:" + IGArr[i]);
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
    
    double criteria = 0.0;
    double MDL_gain = 0.0;
    boolean leftsubset = false;
    int FTi = 0;
    

    public static boolean isStringDouble(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
      }
   
}
