/*
 * Copyright (C) 2011 ankus (http://www.openankus.org).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.ankus.mapreduce.algorithms.clustering.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.ankus.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 클러스터 구조를 관리함.
 * <br>class for cluster info structure and management  used in KMeansDriver
 * @version 0.0.1
 * @date : 2013.08.22
 * @author Moonie
 */
public class KMeansClusterInfoMgr {
	public static String mNominalDelimiter = "@@";
	private int mClusterId = -1;           // cluster identifier
	
	private HashMap<Integer, Double> mNumericValueList;                     // numeric features info of cluster
	private HashMap<Integer, HashMap<String, Double>> mNominalValueList;    // nominal features info of cluster

    /**
    *initialize class object
    *@author Wonmoon
    */
	public KMeansClusterInfoMgr()
	{
		mNumericValueList = new HashMap<Integer, Double>();
		mNominalValueList = new HashMap<Integer, HashMap<String, Double>>();
	}

    /**
    * 클러스터 아이디 설정.<br>
    * @param int id      클러스터 번호.
    * @author Wonmoon
    */
	public void setClusterID(int id)
	{
		mClusterId = id;
	}

    /**
    * 클러스터에 새로운 데이터 할당하면서 발생 빈도를 누적한다
    * @param int attr_index       data index
    * @param String value       value to update
    * @param String dataType    data type - numeric or nominal
    * @return boolean if non-numeric/nominal data, then return falses
    * @author Wonmoon      
    */
	public boolean addAttributeValue(int attr_index, String attr_value, String dataType)
	{
		if(dataType.equals(Constants.DATATYPE_NUMERIC))
		{
			double val = Double.parseDouble(attr_value);
			if(mNumericValueList.containsKey(attr_index)) 
			{
				val += mNumericValueList.get(attr_index);
			}
			mNumericValueList.put(attr_index, val);
		}
		else if(dataType.equals(Constants.DATATYPE_NOMINAL))
		{
			if(mNominalValueList.containsKey(attr_index))
			{
				HashMap<String, Double> attrList = mNominalValueList.get(attr_index);
				if(attrList.containsKey(attr_value)) 
					attrList.put(attr_value, attrList.get(attr_value) + 1.0);
				else
				{
					attrList.put(attr_value, 1.0);
					mNominalValueList.put(attr_index, attrList);
				}
			}
			else //범주형 속성 목록에 없는 것도 범주형 목록에 추가함.
			{
				HashMap<String, Double> newAttr = new HashMap<String, Double>();
				newAttr.put(attr_value, 1.0);
				mNominalValueList.put(attr_index, newAttr);
			}
		}
		else //타입이 기술되지 않은 경우 (사실상 오류임).
			return false;
		
		return true;
	}

    /**
     * 해당 군집의 각 속성에 대해 평균을 산출.
     * @param int dataCnt: 군집에 포함된 데이터 수.
     * @author Wonmoon
     */
	public void finalCompute(int dataCnt)
	{
		Iterator<Integer> numericKeySetIter = mNumericValueList.keySet().iterator();
		while(numericKeySetIter.hasNext())
		{
			int key = numericKeySetIter.next();
			mNumericValueList.put(key, mNumericValueList.get(key) / (double)dataCnt);
		}
		
		Iterator<Integer> nominalKeySetIter = mNominalValueList.keySet().iterator();
		while(nominalKeySetIter.hasNext())
		{
			int key = nominalKeySetIter.next();			
			HashMap<String, Double> valueMap = mNominalValueList.get(key);
			
			Iterator<String> valueKeyIter = valueMap.keySet().iterator();
			while(valueKeyIter.hasNext())
			{
				String valueKey = valueKeyIter.next();
				valueMap.put(valueKey, valueMap.get(valueKey) / (double)dataCnt);
			}			
			mNominalValueList.put(key, valueMap);
		}
	}

    /**
    *파일로 기록할 클러스터 정보 생성. <br> generate string value for file writing of this cluster info
    * @param  String delimiter       delimiter for attribute separation
    * @return String value of cluster info
    * @author Wonmoon
    */
	public String getClusterInfoString(String delimiter)
	{
		// id setting
		String retStr = mClusterId + "";
		
		// numeric value setting
		Iterator<Integer> numericKeySetIter = mNumericValueList.keySet().iterator();
		while(numericKeySetIter.hasNext())
		{
			int key = numericKeySetIter.next();
			retStr += delimiter + key + delimiter + mNumericValueList.get(key);
		}		
		
		// nominal value setting 
		Iterator<Integer> nominalKeySetIter = mNominalValueList.keySet().iterator();
		while(nominalKeySetIter.hasNext())
		{
			int key = nominalKeySetIter.next();			
			HashMap<String, Double> valueMap = mNominalValueList.get(key);
			
			Iterator<String> valueKeyIter = valueMap.keySet().iterator();
			String valueValueStr = "";
			while(valueKeyIter.hasNext())
			{
				String valueKey = valueKeyIter.next();
				valueValueStr += mNominalDelimiter + valueKey + mNominalDelimiter + valueMap.get(valueKey);
			}
			
			retStr += delimiter + key + delimiter + valueValueStr.substring(mNominalDelimiter.length());
		}
		
		return retStr;
	}

    /**
    *  문자열 클러스터 정보를 파싱하여 메모리 저장. <br>load cluster info from cluster info string
    *  @param  String inputStr        string types cluster info
    *  @param  String delimiter       delimiter for attribute separation
    *  @author Wonmoon
    */
	public void loadClusterInfoString(String inputStr, String delimiter)
	{
		mNumericValueList = new HashMap<Integer, Double>();
		mNominalValueList = new HashMap<Integer, HashMap<String,Double>>();
		
		String tokens[] = inputStr.split(delimiter);
		
		mClusterId = Integer.parseInt(tokens[0]);
		
		for(int i=1; i<tokens.length; i++)
		{
			int attrIndex = Integer.parseInt(tokens[i++]);
			String attrValue = tokens[i];
			
			if(!attrValue.contains(mNominalDelimiter)) mNumericValueList.put(attrIndex, Double.parseDouble(attrValue));
			else
			{
				// nominal
				String subTokens[] = attrValue.split(mNominalDelimiter);
				HashMap<String, Double> valueMap = new HashMap<String, Double>();
				for(int k=0; k<subTokens.length; k++)
				{
					String subKey = subTokens[k++];
					double subValue = Double.parseDouble(subTokens[k]);
					valueMap.put(subKey, subValue);
				}
				mNominalValueList.put(attrIndex, valueMap);
			}
		}
	}

    /**
    * 모든 클러스터 정보 로드  <br>load all cluster info from cluster info file
    * @param Configuration conf            configuration identifier
    * @param Path clusterPath     file path for loading
    * @param int clusterCnt      cluster count to load
    * @param String delimiter       delimiter for attribute separation
    * @return KMeansClusterInfoMgr[] cluster info array
    * @author Wonmoon
    */
	public static KMeansClusterInfoMgr[] loadClusterInfoFile(Configuration conf, Path clusterPath, int clusterCnt, String delimiter) throws IOException
	{
		KMeansClusterInfoMgr[] clusters = new KMeansClusterInfoMgr[clusterCnt];
		
		FileStatus[] status = FileSystem.get(conf).listStatus(clusterPath);		
		for(int i=0; i<status.length; i++)
		{
            if(!status[i].getPath().toString().contains("part-r-")) continue;

			FSDataInputStream fin = FileSystem.get(conf).open(status[i].getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(fin, Constants.UTF8));
			String readStr;
			while((readStr = br.readLine())!=null)
			{	
				int clusterId = Integer.parseInt(readStr.substring(0, readStr.indexOf(delimiter)));
				clusters[clusterId] = new KMeansClusterInfoMgr();
				clusters[clusterId].loadClusterInfoString(readStr, delimiter);
			}
			br.close();
			fin.close();
		}
		
		return clusters;
	}

    /**
    * 선택한 속성 입력 값과 클러스터간의 거리를 구함. 
    * @param int attrIndex     비교할 속성 인덱스.
    * @param String attrValue     비교할 값.
    * @param String attrType      데이터 타입.
    * @return double type distance value
    * @author Wonmoon
    */
	public double getAttributeDistance(int attrIndex, String attrValue, String attrType)
	{
		if(attrType.equals(Constants.DATATYPE_NUMERIC))
		{
			if(mNumericValueList.containsKey(attrIndex)) 
			{
				//기존 클러스터의 속성 값 - 입력 데이터의 속성 값.
				return mNumericValueList.get(attrIndex) - Double.parseDouble(attrValue);
			}
			else 
			{
				return Double.parseDouble(attrValue);
			}
		}
		else if(attrType.equals(Constants.DATATYPE_NOMINAL))
		{
			//군집이 특정 속성 번호를 가지는지 검사.
			if(mNominalValueList.containsKey(attrIndex))
			{
				//특정 속성 번호가 가지는 범주 값과 빈도를 가지는 HASH획득.
				HashMap<String, Double> valueMap = mNominalValueList.get(attrIndex);
				//1 - 속성이 가진 범주 값의 갯수
				if(valueMap.containsKey(attrValue))
				{
					return 1 - valueMap.get(attrValue);
				}
				else return 1;
			}
			else return 1;
		}
		
		return 1;
	}

    /**
    *  클러스터 중심의 이동 크기가 수렴하는지 검사함.
    * @param KMeansClusterInfoMgr newCluster     비교할 대상 클러스터
    * @param double convergeRate 클러스터 수렴 값
    * @return boolean if centers of clusters are equal then return true
    * @author Wonmoon
    */
	public boolean isEqualClusterInfo(KMeansClusterInfoMgr newCluster, double convergeRate)
	{
		Iterator<Integer> numericAttrIndexIter = mNumericValueList.keySet().iterator();
		while(numericAttrIndexIter.hasNext())
		{
			int attrIndex = numericAttrIndexIter.next();

            // TODO:  convergeRate compare
            double orgVal = mNumericValueList.get(attrIndex);
            double newVal = newCluster.mNumericValueList.get(attrIndex);
            double gap = Math.abs(orgVal - newVal);

            if(gap > (orgVal * convergeRate)) return false;
		}
		
		
		Iterator<Integer> nominalAttrIndexIter = mNominalValueList.keySet().iterator();
		while(nominalAttrIndexIter.hasNext())
		{	
			int attrIndex = nominalAttrIndexIter.next();

            // TODO: convergenceRate compare
            HashMap<String, Double> orgValueMap = mNominalValueList.get(attrIndex);
            HashMap<String, Double> newValueMap = newCluster.mNominalValueList.get(attrIndex);

            Iterator<String> orgValueNameIter = orgValueMap.keySet().iterator();
            while(orgValueNameIter.hasNext())
            {
                String name = orgValueNameIter.next();

                if(!newValueMap.containsKey(name))
                {
                    double orgVal = orgValueMap.get(name);
                    if(orgVal > convergeRate) return false;
                }
                else
                {
                    double orgVal = orgValueMap.get(name);
                    double newVal = newValueMap.get(name);
                    double gap = Math.abs(orgVal - newVal);
                    if(gap > (orgVal * convergeRate)) return false;
                }
            }

            Iterator<String> newValueNameIter = newValueMap.keySet().iterator();
            while(newValueNameIter.hasNext())
            {
                String name = newValueNameIter.next();
                if(!orgValueMap.containsKey(name))
                {
                    double newVal = newValueMap.get(name);
                    if(newVal > convergeRate) return false;
                }
            }
		}		
		return true;
	}
}
