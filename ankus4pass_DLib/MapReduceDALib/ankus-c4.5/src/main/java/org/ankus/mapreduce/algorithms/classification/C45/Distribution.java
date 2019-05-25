package org.ankus.mapreduce.algorithms.classification.C45;

import java.util.ArrayList;

public class Distribution implements Cloneable 
{
	public  double m_perClassPerBag[][];
	public  double m_perBag[];
	public  double m_perClass[];
	
	protected double totaL;
	  
	public double total()
	{
		  return totaL;
	}
	public final int numBags()
	{
	    return m_perBag.length;
	}
	
	public final int numClasses()
	{
	    return m_perClass.length;
	}
	public final double perClassPerBag(int bagIndex, int classIndex)
	{
		  return m_perClassPerBag[bagIndex][classIndex];
	}
	public final double perBag(int bagIndex)
	{
	    return m_perBag[bagIndex];
	}
	  //Bags : SubTrees 
	  //numClasses : Numbers of Class Kind
	public Distribution(int numBags, int numClasses)
	{
		int i;
		m_perClassPerBag = new double[numBags][0];
		m_perBag = new double[numBags];
		m_perClass = new double[numClasses];
		for (i = 0; i < numBags; i++)
		{
			m_perClassPerBag[i] = new double[numClasses];
		}	
		totaL = 0;
	}
	public Distribution clone() throws CloneNotSupportedException{
		Distribution distribution = (Distribution) super.clone();
		distribution.m_perClassPerBag = this.m_perClassPerBag;
		distribution.m_perBag = this.m_perBag;
		distribution.m_perClass = this.m_perClass;
		
		return distribution;
	}
}
