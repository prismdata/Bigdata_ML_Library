package org.ankus.mapreduce.algorithms.clustering.FuzzyCMeans;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.ankus.util.ArgumentsConstants;
import org.ankus.util.CommonMethods;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 학습에 사용한 데이터에 대하여 소속 군집의 번호를 출력한다.
 * @author HongJoong.Shin
 * @date 2016.12.06
 */
public class Mapper_ClusterAssign extends Mapper<Object, Text, NullWritable, Text>{
	private int mb = 1024*1024;
    private Logger logger = LoggerFactory.getLogger	(Mapper_ClusterAssign.class);
    private int m_indexArr[];	
    private int m_nominalIndexArr[];
    private int m_exceptionIndexArr[];
    private int cluster_count = 0;
    private String delimiter = "";
    private int class_idx = 0;
    private  List<Membership> listMembership = new ArrayList<Membership>();
    /**
     * 구분자, 군집에 사용할 변수 인덱스, 제외할 인덱스를 획득한다.
     * @author HongJoong.Shin
     * @date 2016.12.06
     * @param Context context : 하둡 환경 변수
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    	Configuration conf = context.getConfiguration();
    	delimiter = conf.get(ArgumentsConstants.DELIMITER);
    	m_indexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.TARGET_INDEX, "-1"));
    	m_exceptionIndexArr = CommonMethods.convertIndexStr2IntArr(conf.get(ArgumentsConstants.EXCEPTION_INDEX,  "-1"));
    }
    /**
     * 입력 데이터가 속할 군집의 번호를 출력한다.
     * @author HongJoong.Shin
     * @date 2016.12.06
     * @param Object key : 입력 스프릿 오프셋
     * @param Text value :입력  데이터 
     * @param Context context : 하둡 환경 변수
     */    
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String strInput = value.toString();
		String[] token = strInput.split("\u0001");
		String orgVector = token[0];
		String[] strMembership = token[1].split(":");
		double[] dblMembership = new double[strMembership.length];
		double dblMax = Double.MAX_VALUE * -1;
		int cluster_index = 0;

		for(int mi = 0; mi < dblMembership.length; mi++)
		{
			double membership = Double.parseDouble(strMembership[mi]);
			
			if(membership > dblMax)
			{
				dblMax = membership;
				cluster_index = mi;
			}
		}
		orgVector = orgVector.substring(0, orgVector.length()-1);
		String[] columns = orgVector.split(delimiter);
		String writeValueStr = "";

		for(int i=0; i<columns.length; i++)
		{
			if(!CommonMethods.isContainIndex(m_exceptionIndexArr, i, false))
            {
               if(CommonMethods.isContainIndex(m_indexArr, i, true))
                {
                    writeValueStr += columns[i] + delimiter;
                }
            }
		}
		logger.info(writeValueStr + cluster_index + "," + token[1]);
		context.write(NullWritable.get(), new Text(writeValueStr + cluster_index ));

	}
	
}
