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

package org.ankus.mapreduce.algorithms.classification.MultilayerPerceptron;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MLP_TestReducer2 extends Reducer<IntWritable, Text, NullWritable, Text>{
	private String delimiter;
	private int classIdx;
    private int classIndexType;
  
    private int numHiddenNodes;
    private int numInputNodes;
    private int numOutputNodes;
    private int numNominal;
    
    private double hiddenWeight[][];
    private double outputWeight[][];
    
    private String g_wHidden;
    private String g_wOutput;
    
    private HashMap<String, Integer> nClassMap;
    private String[] nClassList;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER);
        delimiter = ",";
        classIdx = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.CLASS_INDEX));
        
        classIndexType =  Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.CLASS_INDEX_TYPE));
        if(classIndexType == 1){
        	nClassMap = new HashMap<String, Integer>();
        	String[] kList = context.getConfiguration().get(ArgumentsConstants.CLASS_NOMINAL_KEY_LIST).split(",");
        	nClassList = kList;
        	for(int i = 0; i < kList.length; i++){
        		nClassMap.put(kList[i], i);
        	}
        } else {
//        	System.out.println("Numeric Class Index");
        }
        
        g_wHidden = context.getConfiguration().get(ArgumentsConstants.HIDDEN_WEIGHT);
//        System.out.println(g_wHidden);
        g_wOutput = context.getConfiguration().get(ArgumentsConstants.OUTPUT_WEIGHT);
        
        numHiddenNodes = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.HIDDEN_NODE_NUM));
        numInputNodes = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.INPUT_NODE_NUM));
        numOutputNodes = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.OUTPUT_NODE_NUM));
        
        buildNN();
        printNode();
    }

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		Iterator<Text> iterator = values.iterator();
		ArrayList<String> dList = new ArrayList<String>();

        while (iterator.hasNext()) 
        {
        	String val = iterator.next().toString();
//        	System.out.println("MLP: "+val);
        	dList.add(val);
//        	System.out.println("MLP2:"+dList.get(dList.size()-1));

        }
        
        int size = dList.size();
        
//        System.out.println(size+"\t"+dList.get(0));
        
        double train[][] = new double[size][numInputNodes];
        double output[][] = new double[size][numOutputNodes];
        
        int tCnt = 0;
        int oCnt = 0;
        
        for( int i = 0; i < size ; i++){
        	String val = dList.get(i);
        	
//        	
        	String[] valList = val.split(",");
        	if(valList.length > 1){
//        		System.out.println(val);
	        	tCnt = 0;
	        	oCnt = 0;
	        	for(int j = 0; j < valList.length; j++){
	        		if(j != valList.length - 1){
	        			train[i][tCnt] = Double.parseDouble(valList[j]);
	        			tCnt++;
	        		} else {
	        			if(classIndexType == 1){
	        				for(int k = 0; k < numOutputNodes; k++){
	        					if(nClassMap.get(valList[j]) == k){
	        						output[i][k] = 1;
	        					} else {
	        						output[i][k] = 0;
	        					}
	        				}
	        			} else {
	        				output[i][oCnt] = Double.parseDouble(valList[j]);
	        				oCnt++;
	        			}
	        		}
	        	}
        	}
        	
        }
		double error = 0;
		double hidden[] = new double[numHiddenNodes];		
		double pOutput[] = new double[numOutputNodes];

		error = mlpTest(size, train, output);
		//error만 나오게 수정 
		
		       
		context.write(NullWritable.get(), new Text(new String().valueOf(error)));
	}

	private int getMaxIndex(double[] list){
		int len = list.length;
		int maxIdx = -1;
		double max = Double.MAX_VALUE * -1;
		
		
		for(int i = 0 ; i < len ; i++){
			if(list[i] > max){
				max = list[i];
				maxIdx = i;
			}
		}
		return maxIdx;
	}
	
	private String printHiddenWeight(){
		StringBuffer retBuf = new StringBuffer();
    	
		for(int i = 0; i < hiddenWeight.length; i++){
    		for(int j = 0; j < hiddenWeight[0].length; j++){
    			retBuf.append(hiddenWeight[i][j]);
    			
    			if( j < hiddenWeight[0].length){
    				retBuf.append(",");
    			}
    		}
    		if( i < hiddenWeight.length){
				retBuf.append("\t");
			}
    	}
		
		return retBuf.toString();
	}
	private String printOutputWeight(){
		StringBuffer retBuf = new StringBuffer();
    	
		for(int i = 0; i < outputWeight.length; i++){
    		for(int j = 0; j < outputWeight[0].length; j++){
    			retBuf.append(outputWeight[i][j]);
    			
    			if( j < outputWeight[0].length - 1){
    				retBuf.append(",");
    			}
    		}
    		if( i < outputWeight.length - 1){
				retBuf.append("\t");
			}
    	}
		
		return retBuf.toString();
	}
	public void printNode(){
		for(int j = 0; j < numHiddenNodes; j++){
			for(int k = 0; k < numInputNodes; k++){
				System.out.println ("히든: "+hiddenWeight[j][k]);
			}
		}
		for(int j = 0; j < numHiddenNodes ; j++){
			
			for(int k = 0; k < numOutputNodes; k++){
				System.out.println ("출력: "+outputWeight[k][j]);
			}

		}
	}
	public double mlpTest(int numTrain, double[][] input, double[][] tOutput){
		
		//기본 신경망 모델 생성
		/**
		 * hidden: 은닉층 노드 수
		 * hiddenDelta: 입력층- 은닉층간 변화폭
		 * output: 출력층 노드 수
		 * outputDelta: 은닉층 - 출력층간 변화폭
		 */
		int cntEpoch = 0;
		
		double error = 0;
		double hidden[] = new double[numHiddenNodes];
		double hiddenDelta[] = new double[numHiddenNodes];
		
		double output[] = new double[numOutputNodes];
		double outputDelta[] = new double[numOutputNodes];
		
		double sum = 0;
		double preDelta = 0.0F;
		
		error = 0; 
		for(int i = 0 ; i < numTrain; i++){
			ComputeOuput(input[i], hidden, output);
			System.out.println(nClassList[getMaxIndex(output)]);
			for(int j = 0; j < numOutputNodes ; j++){
				error += 0.5 * (output[j] - tOutput[i][j]) * (output[j] - tOutput[i][j]);
				//confusion matrix table을 위한 데이터 테이블 작성 
				
			}
		}
		return error;
	}
	
	/**
	 * 신경망 결과 계산
	 * @param object
	 * @param hidden
	 * @param output
	 * @param numInputNodes
	 * @param numHiddenNodes
	 * @param numOutputNodes
	 */
	private void ComputeOuput(double input[], double[] hidden, double[] output) {
		// TODO Auto-generated method stub
		//결과 출력 계산 함수
//		double input[] = (double []) object;
		double sum = 0;
		
		for(int i = 0; i < numHiddenNodes; i++){
			sum = 0;
//			System.out.println(input.length +" "+hiddenWeight.length+" "+hiddenWeight[i].length);
			for(int j = 0; j < numInputNodes; j++){
//				System.out.println(input.length+"\t"+j);
				sum += input[j] * hiddenWeight[i][j];
			}
			sum += hiddenWeight[i][numInputNodes]; //bias 노드 
			hidden[i] = activationFS(sum);
		}
		for(int i = 0; i < numOutputNodes; i++){
			sum = 0;
			for(int j = 0; j < numHiddenNodes; j++){
				sum += hidden[j] * outputWeight[i][j];
			}
			sum += outputWeight[i][numHiddenNodes]; //bias 노드 
			output[i] = activationFS(sum);
		}
	}
	
	/**
	 * 시그모이드 활성함수
	 * @param val
	 */
	public double activationFS(double val){
		return 1/(1+Math.exp(-val));
	}
	
	private void buildNN(){		
		hiddenWeight = new double[numHiddenNodes][numInputNodes+1];
		outputWeight = new double[numOutputNodes][numHiddenNodes+1];
		
		String wList[] = g_wHidden.split("\t");
		int len = wList.length;
//		System.out.println(numHiddenNodes+"\t"+numInputNodes+"\t"+g_wHidden);
		for ( int i = 0; i < len; i++){
//			System.out.println(wList[i]);
			String wNodeList[] = wList[i].split(",");
			int subLen = wNodeList.length;
			for(int j = 0; j < subLen ; j++){
				hiddenWeight[i][j] = Double.parseDouble(wNodeList[j]);

			}
		}
		
		wList = g_wOutput.split("\t");
		len = wList.length;
		
		for ( int i = 0; i < len; i++){
			String wNodeList[] = wList[i].split(",");
			int subLen = wNodeList.length;
			for(int j = 0; j < subLen ; j++){
				outputWeight[i][j] = Double.parseDouble(wNodeList[j]);
			}
		}
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
