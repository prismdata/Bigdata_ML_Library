package org.ankus.mapreduce.algorithms.clustering.FuzzyCMeans;


import java.io.BufferedReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 입력 벡터에 클러스터 가중치를 적용하여 Reducer로 전달
 * @author HongJoong.Shin
 * @date 2016.12.06
 */
public class Mapper_Centroid extends Mapper<Object, Text, Text, Text>{
	private int mb = 1024*1024;
    private Logger logger = LoggerFactory.getLogger(Mapper_Centroid.class);
    private int m_indexArr[];
    private int m_numericIndexArr[];
    private int m_exceptionIndexArr[];
    private int cluster_count = 0;
    private String delimiter = "";
    /**
     * 변수 구분자, 군집에 사용할 변수 인덱스,예상하는 클러스터의 갯수를 설정한다.
     * @author HongJoong.Shin
     * @date 2016.12.06
     * @param Context context : 하둡 환경 변수
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    	Configuration conf = context.getConfiguration();
    	delimiter = conf.get(ArgumentsConstants.DELIMITER);
    	//indexList
    	m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
    	//Numbers of Cluster
    	cluster_count = conf.getInt(ArgumentsConstants.K_CNT, 1);
    }
    /**
     * 원본 벡터, 클러스터별 가중치를 받아 
     * 클러스터 키 , 가중치가 적용된 벡터로 변환된 값을 출력한다.
     * @author HongJoong.Shin
     * @date 2016.12.06
     * @param Object key : 입력 스프릿 오프셋 
     * @param Text value : 입력 스프릿(원본 벡터, 클러스터별 가중치)
     * @param Context context : 하둡 환경 변수
     */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		//입력 스프릿을 특수 구분자로 분리하여 instance_weight배열에 저장함.
		String[] instance_weight = value.toString().split("\u0001");
		
		//instance_weight의 첫요소는 입력 벡터임.
		String strVector = instance_weight[0];
		
		//입력 벡터를 구분자로 분리하여 입력 백터 배열 strElement에 저장함.
		String[] strElement = strVector.split(delimiter);		
		
		//instance_weight의 두번째 요소인 가중치 목록을 strWeight에 저장함.
		String[] strWeight = instance_weight[1].split(":");
		
		//군집에 사용할 변수 인덱스 크기 만큼 배열 확보
		double[] dblWeightedElmt = new double[m_indexArr.length];
		
		int wvi = 0;
		for(int k = 0; k < cluster_count; k++)
		{
			wvi = 0;
			for(int xindex = 0; xindex < strElement.length; xindex++)
			{
				if(CommonMethods.isContainIndex(m_indexArr,xindex , true))
				{
					NumberFormat formatter = new DecimalFormat("#0.000");					
					//입력 벡터 배열의 각 요소에 접근 
					double element = Double.parseDouble(strElement[xindex]);
					//가중치 벡터의 각 요소에 접근(요소 인덱스는 클러스터 인덱스)
					double weight = Double.parseDouble(strWeight[k]);
					
					//가중치를 적용한 입력 벡터를 생성 
					dblWeightedElmt[wvi] = element * Math.pow(weight, 2);
					formatter.format(dblWeightedElmt[wvi]);
					
					//클러스터 번호, 입력 벡터 인덱스, 클러스커 가중치, 가중치가 적용된 입력 벡터를 출력
					context.write(new Text(k+""), new Text(xindex + "\u0001" + weight +"\u0002" +  formatter.format(dblWeightedElmt[wvi])));
					wvi++;
				}
			}
		}
	
	}
   
}
