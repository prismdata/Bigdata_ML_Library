package org.ankus.mapreduce.algorithms.clustering.FuzzyCMeans;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 군집의 가중치에 대한 구조를 정의한다.
 * @author HongJoong.Shin
 * @date 2016.12.06
 */
class Centroid
{
	/**
	 * <font face="verdana" color="green">군집 가중치</font> 
	 */
	double weight = 0.0;
	/**
	 * <font face="verdana" color="green">군집의 속성 가중치</font>
	 */
	double weightedAttribute = 0.0;
}

/**
 * Mapper에서 계산된 클러스터 가중치와 가중치가 적용된 입력 벡터를 이용하여 각 군집이 가지는 속성 번호와 가중치가 적용된 속성값을 출력한다.<p>
 * 속성값은 가중치가 적용된 속성값 / 클러스터 가중치으로 결정한다.  
 * @author HongJoong.Shin
 * @date 2016.12.06
 */
public class Reducer_Centroid extends Reducer<Text, Text, Text, Text> {
	private Logger logger = LoggerFactory.getLogger(Reducer_Centroid.class);
	private String delimiter = "";
	/**
     * 변수 구분자를 설정한다.
     * @author HongJoong.Shin
     * @date 2016.12.06
     * @param Context context : 하둡 환경 변수
     */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		logger.info("setup");
		Configuration conf = context.getConfiguration();
		delimiter = conf.get(ArgumentsConstants.DELIMITER);
	}
	/**
     * 각 군집이 가지는 속성 번호와 가중치가 적용된 속성값을 출력한다.  
     * @author HongJoong.Shin
     * @date 2016.12.06
     * @param Text CluserK : 클러스터 번호
     * @param Iterable<Text> weightNwVector : "입력 벡터 인덱스, 클러스커 가중치, 가중치가 적용된 입력 벡터"형태의 Iterable 리스트.
     * @param Context context : 하둡 환경 변수
     */
	protected void reduce(Text CluserK, Iterable<Text> weightNwVector, Context context) throws IOException, InterruptedException
	{
		String ClusterKey = CluserK.toString();

		//index마다 Summary of Weight, WeightedAttribute를 계산.
		//weightNwVector에 개별 개체의 모든 속성 정보가 들어가 있음.
		//출력은 Cluster ID, index, new Centroid가 됨.
		HashMap<Integer , Centroid> hashCentroid = new HashMap<Integer, Centroid>();
		int Attr_Idx = 0;
		for(Text wNwe: weightNwVector)
		{
//			****split weight  weighted element			
			String[] indexNWeigthEleVector  = wNwe.toString().split("\u0001");
			Attr_Idx = Integer.parseInt(indexNWeigthEleVector[0]);
			
			String[] w_we = indexNWeigthEleVector[1].split("\u0002");
			double weight = Double.parseDouble(w_we[0]);
			
			if(hashCentroid.containsKey(Attr_Idx) == true)
			{
				Centroid centroid = hashCentroid.get(Attr_Idx);
				centroid.weight += Math.pow(weight, 2);
				centroid.weightedAttribute += Double.parseDouble(w_we[1]);
			}
			else
			{
				Centroid centroid = new Centroid();
				centroid.weight += Math.pow(weight, 2);
				centroid.weightedAttribute += Double.parseDouble(w_we[1]);
				hashCentroid.put(Attr_Idx, centroid);
			}
		}
		for( Map.Entry<Integer, Centroid> ClusterCentroid : hashCentroid.entrySet() )
		{
			Attr_Idx = ClusterCentroid.getKey();
			
			Centroid centroid = ClusterCentroid.getValue();
			double wa = centroid.weightedAttribute;
			double w = centroid.weight;
			DecimalFormat format = new DecimalFormat("0.####");
			String formNum = format.format(wa/w);
			context.write(new Text(ClusterKey), new Text(Attr_Idx + "," + formNum));
        }
         
	}
}
