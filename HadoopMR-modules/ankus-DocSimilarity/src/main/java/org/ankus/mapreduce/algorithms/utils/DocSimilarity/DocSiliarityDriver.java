package org.ankus.mapreduce.algorithms.utils.DocSimilarity;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 문서 유사도 계산을 위한 클래스
 * @author HongJoong.Shin
 * @date :  2016.12.06
 */
public class DocSiliarityDriver extends Configured implements Tool {
	private Logger logger = LoggerFactory.getLogger(DocSiliarityDriver.class);
	
	long endTime = 0;
   	long lTime  = 0;
   	long startTime = 0 ; 
   	/**
   	 * main()함수로 ToolRunner를 사용하여 문서 유사도 분석 기능을 호출한다.
   	 * @author HongJoong.Shin
   	 * @date 2016.12.06
   	 * @param String args[] : 문서 유사도 분석 알고리즘 수행 인자.
   	 * @throws Exception
   	 */
   	public static void main(String args[]) throws Exception 
	{
		int res = ToolRunner.run(new DocSiliarityDriver(), args);
        System.exit(res);
	}
    /**
     * ToolRunner에서 호출되는 실제 알고리즘 시작 함수.
     * @author HongJoong.Shin
     * @date 2016.12.06
     * @param String[] args : 문서 유사도  분석 알고리즘 수행 인자.
     * @return int (정상 종료시 0을 비정상 종료시 1을 리턴)
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
		//입력 경로의 파일 수를 획득함.
		Path path = new Path(conf.get(ArgumentsConstants.INPUT_PATH));
		FileStatus[] status = fs.listStatus(path);
		int file_count = status.length;
	
		Job job_getTerm = new Job(this.getConf());		
		FileInputFormat.addInputPaths(job_getTerm, conf.get(ArgumentsConstants.INPUT_PATH));
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_ALLTERM"), true);//FOR LOCAL TEST
		FileOutputFormat.setOutputPath(job_getTerm, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_ALLTERM"));
		
		/**
		 * 각 문서내의 단어가 중복되 않도록 추출함.
		 */
		job_getTerm.setJarByClass(DocSiliarityDriver.class);
		job_getTerm.setMapperClass(Mapper_Unique_Term.class);
		job_getTerm.setReducerClass(Reducer_Unique_Term.class);
		job_getTerm.setOutputKeyClass(Text.class);
		job_getTerm.setOutputValueClass(IntWritable.class);  
        if(!job_getTerm.waitForCompletion(true))
        {
            logger.info("Error: Get Terms for TF-IDF(Rutine) is not Completeion");
            return 1;
        }
        //문서의 수를 확언 설졍 변수에 저장한다.
        conf.setInt("FILECOUNT", file_count);
        
        Job job_IDF = new Job(this.getConf());
        FileInputFormat.addInputPaths(job_IDF, conf.get(ArgumentsConstants.INPUT_PATH));
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_IDF"), true);//FOR LOCAL TEST
		FileOutputFormat.setOutputPath(job_IDF, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_IDF"));
		
		/*
		 * 문서 파일들로부터 IDF값을 산출한다.
		 */
		job_IDF.setJarByClass(DocSiliarityDriver.class);
		job_IDF.setMapperClass(IDF_Mapper.class);
		job_IDF.setReducerClass(IDF_Reducer.class);
		job_IDF.setOutputKeyClass(Text.class);
		job_IDF.setOutputValueClass(Text.class);  
        if(!job_IDF.waitForCompletion(true))
        {
            logger.info("Error: Get Terms for TF-IDF(Rutine) is not Completeion");
            return 1;
        }
        
        path = new Path(conf.get(ArgumentsConstants.INPUT_PATH));
		status = fs.listStatus(path);
		HashMap<String, String[]>doc_token = new HashMap<String, String[]>();
	
		for(int i = 0; i < status.length; i++)
		{
			if(fs.isFile(new Path(status[i].getPath().toString())) == false)
			{
				continue;
			}
			String eachpath =status[i].getPath().toString();
			String file_name = status[i].getPath().getName();			
			Path doc_path = new Path(eachpath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(doc_path)));
			String line = "";
			StringBuilder sb = new StringBuilder();
			while((line = br.readLine())!= null)
			{
				sb.append(line);
			}
			br.close();
			String[] tokenizedTerms = sb.toString().replaceAll("[\\W&&[^\\s]]", "").split("\\W+");

			//각 문서가 가진 단어들을 HashMap으로 구성한다.
			//Key : 파일명, Value : 단어 배열 
			doc_token.put(file_name, tokenizedTerms);
			
		}
		//모든 문서에 대하여 단어의 갯수를 획득한다.
		for(int i = 0; i < status.length; i++)
		{
			if(fs.isFile(new Path(status[i].getPath().toString())) == false)
			{
				continue;
			}
			String[] tokenizedTerms = doc_token.get(status[i].getPath().getName());
			//각  문서가 가지는 단어이 갯수를 획득하고 환경 변수에 저장한다.
			conf.setInt("WC", tokenizedTerms.length);
			Job job_TF = new Job(this.getConf());
			String eachpath =status[i].getPath().toString();
			String file_name = status[i].getPath().getName();
	        FileInputFormat.addInputPaths(job_TF, eachpath);	        
	        int extension_start = file_name.lastIndexOf(".");
	        String pure_file = "";
	        int extension_length = 0;
	        if(extension_start == -1)
	        {
	        	pure_file = file_name;
	        }
	        else
	        {
	        	extension_length = file_name.length() - extension_start;
				pure_file = file_name.substring(0,extension_start);
	        }
			if(pure_file.length() == 0)
			{
				continue;
			}
			//각 문서 파일마다 TF를 계산하고 결과를 출력 폴더의 파일명_TF로 저장한다.
			FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/"+pure_file+"_TF"), true);//FOR LOCAL TEST
			FileOutputFormat.setOutputPath(job_TF, new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/"+pure_file+"_TF"));

			/**
			 * 현재 문서에 대한 TF IDF를 산출한다.
			 */
			job_TF.setJarByClass(DocSiliarityDriver.class);
			job_TF.setMapperClass(TF_Mapper.class);
			job_TF.setReducerClass(TF_Reducer.class);
			job_TF.setOutputKeyClass(Text.class);
			job_TF.setOutputValueClass(Text.class);  
	        if(!job_TF.waitForCompletion(true))
	        {
	            logger.info("Error: Get Terms for TF-IDF(Rutine) is not Completeion");
	            return 1;
	        }
	        
		}
		/**
		 * 저장된 각 문서의 단어에 대한 TF-IDF값을 로드하여 벡터로 형성하고 Cosine함수를 이용하여
		 * 문서간의 유사도를 산출한다. 
		 */
		FSDataOutputStream fout = fs.create(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH) + "/similarity_result.csv"), true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fout, Constants.UTF8));
        //파일 목록으로 부터 비교 원본에 대한 문서 백터를 형성.
        String[] file_list = new String[status.length];
        
		for(int i = 0; i < status.length; i++)
		{
			if(fs.isFile(new Path(status[i].getPath().toString())) == false)
			{
				continue;
			}
			String file_name = status[i].getPath().getName();
			int extension_length = 0;
			String pure_file = "";
			
			int extension_start = file_name.lastIndexOf(".");
			if(extension_start == -1)
			{
				pure_file = file_name;
			}
			else
			{
			extension_length = file_name.length() - extension_start;
			pure_file = file_name.substring(0,extension_start);
			}
			if(pure_file.length() == 0)
			{
				continue;
			}
			file_list[i] = pure_file;
			String file_path = conf.get(ArgumentsConstants.OUTPUT_PATH)+"/"+pure_file+"_TF";
			FileStatus[] r_status = fs.listStatus(new Path(file_path));
			
			/*
			 * 파일 목록으로 부터 비교 대상에 대한 문서 백터를 형성.
			 */
			List<Double> WordVector_Src = load_WordVectorList(fs, r_status);			
			HashMap<String, Double> doc_sim_map = new HashMap<String, Double>();			
			for(int j = 0; j < status.length; j++)
			{
				if(fs.isFile(new Path(status[j].getPath().toString())) == false)
				{
					continue;
				}
				String file_namej = status[j].getPath().getName();
				if(file_name.equals(file_namej) == true)
					continue;
				extension_start = file_namej.lastIndexOf(".");
				if(extension_start == -1)
				{
					pure_file = file_name;
				}
				else
				{
					extension_length = file_namej.length() - extension_start;
					pure_file = file_namej.substring(0,extension_start);
				}
				
				if(pure_file.length() == 0)
				{
					continue;
				}
				String file_pathj = conf.get(ArgumentsConstants.OUTPUT_PATH)+"/"+pure_file+"_TF";
				FileStatus[] r_statusj = fs.listStatus(new Path(file_pathj));
				
				//워드 벡터로 부터 코아인 거리를 구하여 문서명, 유사도로 구성 HashMap에 내용을 저장. 
				List<Double> WordVector_Target = load_WordVectorList(fs, r_statusj);
				double similarity = 0.0, up = 0.0;
				for(int wi = 0; wi < WordVector_Src.size(); wi++)
				{
					up += WordVector_Src.get(wi) * WordVector_Target.get(wi);
				}
				
				double down_A = 0.0, down_B = 0.0;
				for(int wi = 0; wi < WordVector_Src.size(); wi++)
				{
					down_A += WordVector_Src.get(wi) * WordVector_Src.get(wi);
				}
				down_A = Math.sqrt(down_A);
				
				for(int wi = 0; wi < WordVector_Target.size(); wi++)
				{
					down_B += WordVector_Target.get(wi) * WordVector_Target.get(wi);
				}
				down_B = Math.sqrt(down_B);
				similarity = up / (down_A * down_B);
				doc_sim_map.put(file_namej, similarity);
				
			}
			Iterator it = sortByValue(doc_sim_map).iterator();
	        //각 문서에 대한 유사도를 파일로 저장.
	        while(it.hasNext()){
	            String temp = (String) it.next();
	            String pattern = "#.###";
	            DecimalFormat dformat = new DecimalFormat(pattern);	            
	            bw.write(file_name +","+temp +"," + dformat.format(doc_sim_map.get(temp)) +"\r\n");
	        }
	        bw.write("\r\n");
			
		}
		bw.close();
		fout.close();
		endTime = System.nanoTime();
		lTime = endTime - startTime;
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_IDF"), true);
		FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"_ALLTERM"), true);
		for(int fi = 0; fi < file_list.length; fi++)
		{
			FileSystem.get(conf).delete(new Path(conf.get(ArgumentsConstants.OUTPUT_PATH)+"/"+ file_list[fi] +"_TF"), true);//FOR LOCAL TEST	
		}
		
		System.out.println("Process Finished TIME(ms) : " + lTime/1000000.0 + "(ms)");
		System.out.println("Process Finished TIME(sec):" + (lTime/1000000.0)/1000);
        return 0;
	}
	/**
	 * HashMap구조로 저장된 데이터를 value를 기반으로 내림 차순 정렬을 수행한다 
	 * @author HongJoong.Shin
	 * @date 2016.12.06
	 * @param HashMap map : 정렬할 데이터
	 * @return List 타입의 정렬된 데이터
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static List sortByValue(final HashMap map){
        List<String> list = new ArrayList();
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
	/**
	 * 주어진 입력 문서에 대한 워드 벡터를 TF-IDF를 시용하여 구성한다.
	 * @author HongJoong.Shin
	 * @date 2016.12.06
	 * @param FileSystem fs : FileSystem
	 * @param FileStatus[] status : 특정 문서 폴더를 포함하는 배열
	 * @return List<Double> 워드 백터 
	 */
	private List<Double> load_WordVectorList(FileSystem fs , FileStatus[] status)
	{
		List<Double> WordVector = new ArrayList<Double>();
		try
    	{
			for(int i=0; i<status.length; i++)
	        {
	            Path fp = status[i].getPath();	            
	            if(fp.getName().indexOf("r")<0) continue;
	            FSDataInputStream fin = fs.open(status[i].getPath());
	            BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));	            
	            String readStr;
	            while((readStr=br.readLine())!=null)
	            {
	               String [] vector = readStr.split("\t");
	               String[] tfidf = vector[3].split(":");	               
	               WordVector.add(Double.parseDouble(tfidf[1]));
	            }
	            br.close();
	            fin.close();
	        }
	        
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.toString());
    	}
		return WordVector;
		
	}
	
}