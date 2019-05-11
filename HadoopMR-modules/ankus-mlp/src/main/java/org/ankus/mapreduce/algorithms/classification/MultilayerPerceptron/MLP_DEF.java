package org.ankus.mapreduce.algorithms.classification.MultilayerPerceptron;

public class MLP_DEF implements java.io.Serializable {
	String[] keyList;
	
	StringBuffer minBuf;
	StringBuffer maxBuf;
	
	String wHidden;
	String wOutput;
	
	   public MLP_DEF(String[] keyList, StringBuffer minBuf, StringBuffer maxBuf,	String wHidden,	String wOutput) 
	   { 
		   this.keyList = keyList;
		   this.minBuf = minBuf;
		   this.maxBuf = maxBuf;
		   this.wHidden = wHidden;
		   this.wOutput = wOutput;
	   } 
}
