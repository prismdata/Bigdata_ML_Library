package org.ankus.mapreduce.algorithms.utils.GraphAnalysis;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.ankus.util.ArgumentsConstants;
import org.ankus.util.ConfigurationVariable;
import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 그래프 분석을 수행하는 클래스 
 * @author HongJoong.Shin
 * @date   2016.xx.xx
 */
public class GraphAnalysisDriver extends Configured implements Tool {
	private Logger logger = LoggerFactory.getLogger(GraphAnalysisDriver.class);
	long endTime = 0;
   	long lTime  = 0;
   	long startTime = 0 ; 
   	
   	/**
     * ToolRunner에서 호출되는 실제 알고리즘 시작 함수.
     * @author HongJoong.Shin
     * @date 2016.xx.xx
     * @parameter String[] args : 그래프 분석 알고리즘 수행 인자.
     * @return int
     */
	public int run(String[] args) throws Exception
	{
		Configuration conf = this.getConf();
		if(!ConfigurationVariable.setFromArguments(args, conf))
		{
			logger.error("MR Job Setting Failed..");
			logger.info("Error: MR Job Setting Failed..: Configuration Failed");
		     return 1;
		}
		startTime = System.nanoTime();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(conf.get(ArgumentsConstants.INPUT_PATH));
		FileStatus[] status = fs.listStatus(path);
		int file_count = status.length;
	
		Job jobGraphAnalysis = new Job(this.getConf());
		
		FileInputFormat.addInputPaths(jobGraphAnalysis, conf.get(ArgumentsConstants.INPUT_PATH));
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)), true);//FOR LOCAL TEST
		FileOutputFormat.setOutputPath(jobGraphAnalysis, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)));
		
		jobGraphAnalysis.setJarByClass(GraphAnalysisDriver.class);
		
		jobGraphAnalysis.setMapperClass(GraphLoad_Mapper.class);
		jobGraphAnalysis.setReducerClass(GraphAnalysis_Reducer.class);
		jobGraphAnalysis.setOutputKeyClass(Text.class);
		jobGraphAnalysis.setOutputValueClass(Text.class);  
        if(!jobGraphAnalysis.waitForCompletion(true))
        {
            logger.info("Error: Get Terms for TF-IDF(Rutine) is not Completeion");
            return 1;
        }
        
        
        //File Renameing..
        //Get Influence
        mFileIntegration(fs, conf.get(ArgumentsConstants.OUTPUT_PATH), "part-r");
        String influ_ext_source_path = conf.get(ArgumentsConstants.OUTPUT_PATH);
        Extension_Fluence(fs,influ_ext_source_path,"influence_basic.txt");
        
//        Structural_Simality(fs,influ_ext_source_path,"influence_basic.txt", "part-r");
        endTime = System.nanoTime();
		lTime = endTime - startTime;
		
		System.out.println("Graph Analys Processing Time(ms) : " + lTime/1000000.0 + "(ms)");
		System.out.println("Graph Analys Processing Time(sec):" + (lTime/1000000.0)/1000 + "(sec)");
		return 0;
	}

	/**
	 * jobGraphAnalysis으로 부터 생성된 직접 연결된 노드들을 확장하여 시작 노드로 부터 접근 가능한 모든 노드를 추적한다.
	 * @author HongJoong.Shin
	 * @date 2016.xx.xx
	 * @param FileSystem fs : 하둡 파일 시스템 정보 
	 * @param String inputPath : 거리가 1인 이웃 노드들 폴더명
	 * @param String basic_name :  거리가 1인 이웃 노드들의 파일명
	 */
	private void Extension_Fluence(FileSystem fs , String inputPath, String basic_name)
	{
		try
    	{
			//Load 
			String tmp = "";
	    	FileStatus[] status = fs.listStatus(new Path(inputPath + "/"+ basic_name));	    	
	    	FSDataOutputStream fout = fs.create(new Path(inputPath + "/influence_ext.txt"), true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
            
            HashMap<String, List> LoadLink = new HashMap<String, List>();
            HashMap<String, List> LoadLink_TMP = new HashMap<String, List>();
	        for(int i=0; i<status.length; i++)
	        {
	            Path fp = status[i].getPath();
	            	
	            FSDataInputStream fin = fs.open(status[i].getPath());
	            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));	            
	            
	            String readStr, tokens[];
	            int value;
	            while((readStr=br.readLine())!=null)
	            {
	              String[] base_info = readStr.split("\t");
	              String startNode = base_info[0].trim();
	              String link = base_info[1].trim();
	              String[] endNodesList = link.split("@@");
	              
	              tmp = endNodesList[1];
	              tmp = tmp.substring(1, tmp.length()-1);
	              
	              String[] aNode = tmp.split(",");
	              List<String> nodes = new ArrayList<String>();
	              for(int aNi = 0 ; aNi < aNode.length; aNi++)
	              {
	            	  nodes.add(aNode[aNi].trim());
	              }
	              
	              LoadLink.put(base_info[0].trim(), nodes);
	              LoadLink_TMP.put(base_info[0].trim(), nodes);
	            }
	            br.close();
	            fin.close();
	        }
	        
	        Iterator<String> startNode = LoadLink.keySet().iterator();
	        
	        System.out.println(LoadLink);
	        while(startNode.hasNext())//Search Full Start Node.
	        {
	            String node = startNode.next();
	            List<String> Source_Node = LoadLink.get(node); //Source_Node
	            System.out.println(node +" Before : " + Source_Node);
	            for(int i = 0; i < Source_Node.size(); i++)
	            {
	            	String eNode  = Source_Node.get(i);
	            	if(LoadLink_TMP.containsKey(eNode) == true)
	            	{
		            	List<String> targetLink = LoadLink_TMP.get(eNode);//Target Node
		            	System.out.println(eNode + " Merge : " + LoadLink_TMP.get(eNode));
		            	for (String x : targetLink)
		            	{
		            		if (!Source_Node.contains(x))
							{
		            			Source_Node.add(x);
		            			//System.out.println("Append:"+x);
							}
		            	}
	            	}
	            }
	            System.out.println("After : " + Source_Node);
	        }       
	        
	        TreeMap<String,List> tm = new TreeMap<String,List>(LoadLink);	        
	        Iterator<String> iteratorKey = tm.keySet( ).iterator( );   //키값 오름차순 정렬(기본)	      
	        while(iteratorKey.hasNext())//Search Full Start Node.
	        {
	        	String node = iteratorKey.next();
	            List<String> Source_Node = LoadLink.get(node); //Source_Node
	            
	            bw.write(node +"\t" + Source_Node.size()+"@@"+ Source_Node+"\n");
	            
	            System.out.println(node +"/"  + Source_Node);
	        }
	        bw.close();
	        fout.close();
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.toString());
    	}
	}
	/**
	 * MR의 결과를 통합한다.
	 * @author HongJoong.Shin
	 * @date 2016.xx.xx
	 * @param FileSystem fs : 하둡 파일 시스템 정보 
	 * @param inputPath  : 통합할 파일 폴더 경로
	 * @param filePrefix : 읽을 파일명 패턴
	 */
	private void mFileIntegration(FileSystem fs , String inputPath, String filePrefix)
    {
    	try
    	{
	    	FileStatus[] status = fs.listStatus(new Path(inputPath));
	    	FSDataOutputStream fout = fs.create(new Path(inputPath + "/influence_basic.txt"), true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
            HashMap<Double, String> influence_score = new HashMap<Double, String>();
            
	        for(int i=0; i<status.length; i++)
	        {
	            Path fp = status[i].getPath();
	            
	            if(fp.getName().indexOf(filePrefix)<0) continue;
	
	            FSDataInputStream fin = fs.open(status[i].getPath());
	            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));	            
	            
	            String readStr, tokens[];
	            int value;
	            while((readStr=br.readLine())!=null)
	            {
	            	bw.write(readStr);
					bw.write("\r\n");
	            }
	            
	            
	            br.close();
	            fin.close();
	            fs.delete(fp);
	        }
	        bw.close();
	        fout.close();
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.toString());
    	}
    }
	/**
     * main()함수로 ToolRunner를 사용하여 그래프 분석 기능을 호출한다.
     * @auth  HongJoong.Shin
     * @parameter String[] args : 그래프 분석 알고리즘 수행 인자.
     * @author HongJoong.Shin
     * @date   2016.xx.xx
     * @return 없음.
     */
	public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new GraphAnalysisDriver(), args);
        System.exit(res);
	}


}