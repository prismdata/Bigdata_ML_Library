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

public class MLP_Reducer2 extends Reducer<IntWritable, Text, NullWritable, Text>{
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

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER);
        delimiter = "/t";
        classIdx = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.CLASS_INDEX));
        
        classIndexType =  Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.CLASS_INDEX_TYPE));
        if(classIndexType == 1){
        	nClassMap = new HashMap<String, Integer>();
        	String[] kList = context.getConfiguration().get(ArgumentsConstants.CLASS_NOMINAL_KEY_LIST).split(",");
        	
        	for(int i = 0; i < kList.length; i++){
        		nClassMap.put(kList[i], i);
        	}
        } else {
        	System.out.println("Numeric Class");
        }
        
        g_wHidden = context.getConfiguration().get(ArgumentsConstants.HIDDEN_WEIGHT);
        g_wOutput = context.getConfiguration().get(ArgumentsConstants.OUTPUT_WEIGHT);
        
        numHiddenNodes = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.HIDDEN_NODE_NUM));
        numInputNodes = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.INPUT_NODE_NUM));
        numOutputNodes = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.OUTPUT_NODE_NUM));
        buildNN();
//        printNode();
    }

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		Iterator<Text> iterator = values.iterator();
		ArrayList<String> dList = new ArrayList<String>();

        while (iterator.hasNext()) 
        {
        	String val = iterator.next().toString();
        	if(val.split(",").length > 1)
        		dList.add(val);
        }
        
        int size = dList.size();
        
        double train[][] = new double[size][numInputNodes];
        double output[][] = new double[size][numOutputNodes];
        
        int tCnt = 0;
        int oCnt = 0;
        
        for( int i = 0; i < size ; i++){
        	String val = dList.get(i);
        	String[] valList = val.split(",");
//        	
        	if(valList.length > 1){
//        		System.out.println("MLP::\t"+val);
	        	tCnt = 0;
	        	oCnt = 0;
	        	
	        	for(int j = 0; j < valList.length; j++){
	        		if(j != valList.length - 1){
	        			train[i][tCnt] = Double.parseDouble(valList[j]);
	        			tCnt++;
	        		} else {
//	        			System.out.println("=========Class Index");
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
//	        				System.out.println("input Training:\t"+output[i][oCnt]);
	        				oCnt++;
	        			}
	        		}
	        		
	        	}
        	}	
        }
        double learningRate = Double.parseDouble(context.getConfiguration().get(ArgumentsConstants.LEARNING_RATE));
        double momentum = Double.parseDouble(context.getConfiguration().get(ArgumentsConstants.MOMENTUN));
        int numEpoch = Integer.parseInt(context.getConfiguration().get(ArgumentsConstants.SUB_MAX_EPOCH));
        
        mlpTrain(learningRate, momentum, numEpoch, size, train, output);
        
//        System.out.println("Training::::");
//        printNode();
        context.write(NullWritable.get(), new Text("h\t"+printHiddenWeight()));
        context.write(NullWritable.get(), new Text("o\t"+printOutputWeight()));
        
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
	
	public void mlpTrain(double learningRate, double momentum, int numEpoch, int numTrain, double[][] input, double[][] tOutput){
		
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
		

//		//초기 가중치 출력
//		printNode();
//		//모멘텀 적용을 위한 직전 기울기 저장용
		double preDelta = 0.0F;
		
		do{ //epoch 수치만큼 반복학습
			error = 0; //에러 초기화
			//신경망 가중치 조정
			//모멘텀 변수 추가
			for(int i = 0 ; i < numTrain; i++){
				//출력값 계산
				//160622 구조 수정 필요
//				System.out.println(tOutput[i][0]+" "+tOutput[i][1]+" "+tOutput[i][2]+" ");
				ComputeOuput(input[i], hidden, output);

				//출력미분값 계산
				for(int j = 0; j < numOutputNodes; j++){
					preDelta = outputDelta[j];
					outputDelta[j] = (output[j] - tOutput[i][j]) * (1 - output[j]) * output[j] * learningRate + momentum * preDelta;
//					outputDelta[j] = learningRate * (output[j] - tOutput[i][j]) * (1 - output[j]) * output[j] + momentum * preDelta;
					//모멘텀 적용
//					outputDelta[j] = momentum * preDelta + ( 1 - momentum ) * outputDelta[j];
				}
				//출력가중치 조정
				for(int j = 0; j < numOutputNodes; j++){
					for(int k = 0; k < numHiddenNodes; k++){
						//16.06.30이전 공식
						outputWeight[j][k] -= outputDelta[j] * hidden[k];
					}
					outputWeight[j][numHiddenNodes] -= outputDelta[j]; //bias Node
				}
				
				//은닉층 미분값 계산
				for(int j = 0; j < numHiddenNodes ; j++){
					sum = 0;
					
					for(int k = 0; k < numOutputNodes; k++){
						sum += outputDelta[k] * outputWeight[k][j];
					}
					preDelta = hiddenDelta[j];
					hiddenDelta[j] = momentum * preDelta + sum * (1 - hidden[j]) * hidden[j];
//					hiddenDelta[j] = sum * (1 - hidden[j]) * hidden[j] + momentum * preDelta;
//					hiddenDelta[j] =  momentum * preDelta + ( 1 - momentum ) * hiddenDelta[j];
				}
				
				//은닉가중치 조정
				for(int j = 0; j < numHiddenNodes; j++){
					for(int k = 0; k < numInputNodes; k++){
						hiddenWeight[j][k] -= hiddenDelta[j] * input[i][k];
					}
					hiddenWeight[j][numInputNodes] -= hiddenDelta[j]; // bias Node
				}
				
				//오차 계산
				for(int j = 0; j < numOutputNodes ; j++){
//					System.out.println(j+"/t"+(0.5 * (output[j] - tOutput[i][j]) * (output[j] - tOutput[i][j])));
					error += 0.5 * (output[j] - tOutput[i][j]) * (output[j] - tOutput[i][j]);
				}
//				if(cntEpoch == numEpoch -1){
//					System.out.println("Traininged:\t"+tOutput[i][0]+"\tPredicted:\t"+output.length+"\t"+output[0]);
//				}
			}
			//모델 업데이트 과정 모니터링
			if(cntEpoch % 10 == 0){
				System.out.println("반복횟수: "+cntEpoch+", 오차: "+error);
			}
		} while( cntEpoch++ < numEpoch); //최대 학습 횟수만큼 학습 
//		System.out.println("::::Training Nodes:::::");
//		printNode();
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
//				System.out.print(input[j]+"\t");
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
//			System.out.println(output[i]);
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
//				System.out.println(len+"\t"+subLen+"\t"+outputWeight[i][j]);

			}
		}
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
