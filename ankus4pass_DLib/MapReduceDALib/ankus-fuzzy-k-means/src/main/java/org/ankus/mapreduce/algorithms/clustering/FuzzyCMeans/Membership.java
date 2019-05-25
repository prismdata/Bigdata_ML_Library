package org.ankus.mapreduce.algorithms.clustering.FuzzyCMeans;

import java.util.HashMap;

/**
 * 군집의 번호와 중심 값 저장을 위한 클래스를 정의함.
 * @author HongJoong.Shin
 * @date 2016.12.06
 */
class Membership
{
	/**
	 * <font face="verdana" color="green">클러스터 번호</font> 
	 */
	int Cluster_id = 0;
	/**
	 * <font face="verdana" color="green">Key: 속성번호,Value: 가중치</font>
	 */
	HashMap<Integer, Double> attribute_mean = null;
	/**
	 * 군집번호와 중심값을 저장하는 클래스를 초기화 함
	 * @author HongJoong.Shin
	 * @date 2016.12.06
	 */
	public Membership()
	{
		attribute_mean = new HashMap<Integer, Double>();
	}
}