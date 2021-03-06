package org.ankus.mapreduce.algorithms.clustering.FuzzyCMeans;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * 이전 군집화 단계에서 생성된 군집의 중심값을 로드하여  군집 중심과 입력 데이터의 거리를 산출하고  각 군집과 데이터 간의 거리의 역수를 사용하여
 * 입력 데이터의 군집 소속 가중치를 산출한다. 
 * @author HongJoong.Shin
 * @date 2016.12.06
 */
@SuppressWarnings("deprecation")
public class Mapper_Membership extends Mapper<Object, Text, Text, Text>{
	private int mb = 1024*1024;
    private Logger logger = LoggerFactory.getLogger(Mapper_Membership.class);
    private int iteration_count = 0;
    private int m_indexArr[];
    private int m_numericIndexArr[];
    private int m_exceptionIndexArr[];
    private int cluster_count = 0;
    private String delimiter = "";

    private List<Membership> Clusters = new ArrayList<Membership>();
    /**
     * 분산 캐쉬에 저장된 이전 군집화 단계의 군집의 중심값 가중치를 로드함.
     * @author HongJoong.Shin
     * @date 2016.12.06
     * @param Context context : 하둡 환경 변수
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    	Configuration conf = context.getConfiguration();
    	iteration_count = conf.getInt("iteration_count", 0);    	
    	delimiter = conf.get(ArgumentsConstants.DELIMITER);
    	//indexList
    	m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
    	//Numbers of Cluster
    	cluster_count = conf.getInt(ArgumentsConstants.K_CNT, 1);
	
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for(int pi = 0; pi < cacheFiles.length; pi++)
		{
			if(cacheFiles != null && cacheFiles.length > 0)
			{
				String line;
				String[] tokens;
				try
				{
					BufferedReader br = new BufferedReader(new FileReader(cacheFiles[pi].toUri().getPath()));
					while((line  = br.readLine())!= null)
					{
						//logger.info(line);
						tokens = line.split("\t");
						String tmp_Cluster = tokens[0];
						String tmp_Centroid = tokens[1];
						/**
						1 listMemberShip 조회하면서 tmp_Cluter가 있는지 검사
						1 y Membership의 Mashmap 획득.
						  1.1 해당 Membership의 변수 인덱스가 있는지 검사
						     1.1.1 y 신규 값을 추가.
						     1.1.1 n MashMap으로 구성하여 Membership에 추가.
						1 n  Memship,  attribute_mean 신규 생성
						  1.1 attribute_mean에 값 설정
						  1.2 Membership설정 (attribute_mean 연결)
						  1.3 listMemberShip에 Membsership추가.
						 */
						int Cluster_id = Integer.parseInt(tmp_Cluster);
						String[] idx_Mean = tmp_Centroid.split(",");
						int idx = Integer.parseInt(idx_Mean[0]);
						double mean = Double.parseDouble(idx_Mean[1]);
						if(Clusters.size() > 0)
						{
							boolean notExist = true;
							for(Membership membership: Clusters)
							{
								if(membership.Cluster_id == Cluster_id)//1
								{
									notExist = false;
									membership.attribute_mean.put(idx, mean); 
								}
							}
							if(notExist == true)
							{
								HashMap<Integer, Double> Cluster_mean = new HashMap<Integer, Double>();//1 n
								Cluster_mean.put(idx, mean);
								Membership newCluster = new Membership();
								newCluster.Cluster_id = Cluster_id;
								newCluster.attribute_mean = Cluster_mean;
								Clusters.add(newCluster);
							}
						}
						else
						{
							HashMap<Integer, Double> Cluster_mean = new HashMap<Integer, Double>();//1 n
							Cluster_mean.put(idx, mean);
							Membership newCluster = new Membership();
							newCluster.Cluster_id = Cluster_id;
							newCluster.attribute_mean = Cluster_mean;
							Clusters.add(newCluster);							
						}
						
					}
				}catch(Exception e)
				{
					logger.info(e.toString());
				}
			}
		}
		logger.info("Membership size:" + Clusters.size());
	
    }
    
    /**
     * 군집 중심과 입력 데이터의 거리를 산출하고  각 군집과 데이터 간의 거리의 역수를 사용하여
     * 입력 데이터의 군집 소속 가중치를 산출한다. 
     * @author HongJoong.Shin
     * @date 2016.12.06
     * @param Object key : 입력 스프릿 오프셋
     * @param Text value :입력  데이터 
     * @param Context context : 하둡 환경 변수
     */
	@Override	
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		
			String raw_vector = value.toString();
			String[] Elements = raw_vector.split(delimiter);
			
			double[] UpdateMembership = new double[Clusters.size()];			
			
			for(Membership membership: Clusters)
			{
				int cluster_id = membership.Cluster_id;
				double InverseDist = 0.0;
				HashMap<Integer, Double> mapIdxMean = membership.attribute_mean;
				double distance_sum = 0.0;
				for(int idx: mapIdxMean.keySet())
				{
					String strElemetns = Elements[idx];
					double src_element = Double.parseDouble(strElemetns);
					double mean = mapIdxMean.get(idx);
					
					distance_sum +=Math.pow(src_element - mean, 2);
				}
				InverseDist += (1 / distance_sum);
				UpdateMembership[cluster_id] = InverseDist;
				//emit : null, Value : rawdata \t new membership by each cluster
			}
			String newClusterMembership = "";
			for(int ui = 0; ui < UpdateMembership.length; ui++)
			{
				double up = UpdateMembership[ui];
				double down = 0.0;
				for(int di = 0; di < UpdateMembership.length; di++)
				{
					down += UpdateMembership[di];
				}
				double newMembership = up / down;
				NumberFormat formatter = new DecimalFormat("#0.000");     
				newClusterMembership += formatter.format(newMembership) + ":";
			}
			newClusterMembership = newClusterMembership.substring(0, newClusterMembership.length()-1);
			String original = value.toString();
			context.write(new Text(original), new Text("\u0001"+newClusterMembership));
	}
	
}
