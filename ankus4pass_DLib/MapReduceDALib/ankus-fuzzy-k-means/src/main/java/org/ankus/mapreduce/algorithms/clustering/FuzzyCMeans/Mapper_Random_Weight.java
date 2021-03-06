package org.ankus.mapreduce.algorithms.clustering.FuzzyCMeans;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 각 속성에 군집 소속 활률을 지정하는 클래스
 * @author HongJoong.Shin
 * @date 2018. 1. 19.
 */
public class Mapper_Random_Weight extends Mapper<Object, Text, Text, Text>{
	
	private int mb = 1024*1024;
    private Logger logger = LoggerFactory.getLogger(Mapper_Random_Weight.class);
    private int cluster_count =1;
    /**
     * 사용자가 지정한 클러스터의 갯수를 얻는다.
     * @author HongJoong.Shin
     * @date 2016.12.06
     * @param Context context : 하둡 환경 변수
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    	Configuration conf = context.getConfiguration();
    	cluster_count = conf.getInt(ArgumentsConstants.K_CNT, 1);
    }
    /**
     * 전체 레코드에 가중치를 벡터를 할당한다.
     * @author HongJoong.Shin
     * @date 2016.12.06
     * @param Object key : 입력 스프릿 오프셋 
     * @param Text vale : 입력 스프릿 
     * @param Context context : 하둡 환경 변수
     */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		try
		{
			String instaneVector = value.toString();
			double[] random_weight = new double[cluster_count];
			
			String clusters_weight = "";
			Random random = new Random();
			
			//1st Random Vlaue Assign
			double SumOfRandom = 0.0;
			for(int ri = 0; ri < cluster_count; ri++)
			{
				double RandomValue =  (double)(random.nextInt(9)+1)/10.0;
				random_weight[ri] = RandomValue;
				SumOfRandom += RandomValue;
			}
			double RandomValibale_Sum = 0.0;
			for(int ri = 0; ri < cluster_count; ri++)
			{
				random_weight[ri]  = random_weight[ri]  / SumOfRandom;
				clusters_weight += random_weight[ri] + ":"; 
				RandomValibale_Sum += random_weight[ri]; 
			}
			if( Math.abs(1- RandomValibale_Sum) > 0.0000000001)
			{
				throw new Exception("초기 확률 오류.");
			}
			context.write(new Text(instaneVector), new Text(clusters_weight));
		}
		catch(Exception e)
		{
			logger.error(e.toString());
		}
	}

}
