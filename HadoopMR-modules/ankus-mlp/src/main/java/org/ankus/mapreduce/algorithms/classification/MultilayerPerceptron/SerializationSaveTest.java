package org.ankus.mapreduce.algorithms.classification.MultilayerPerceptron;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

public class SerializationSaveTest {
	public static void main(String[] args){
		SerializationSaveTest sst = new SerializationSaveTest();
		sst.test();
	}
	
	public void test(){
		try{   
            FileOutputStream fos =
                                 new FileOutputStream("file.out"); 
            ObjectOutputStream oos =
                                 new ObjectOutputStream(fos); 
            oos.writeObject(new Test("testing", 37)); 
            oos.flush(); 
            fos.close(); 
        } 
        catch(Throwable e)  
        { 
            System.err.println(e); 
        }    
	}
}
