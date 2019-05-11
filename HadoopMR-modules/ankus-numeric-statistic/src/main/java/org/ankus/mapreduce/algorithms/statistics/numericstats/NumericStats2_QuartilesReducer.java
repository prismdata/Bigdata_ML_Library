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

package org.ankus.mapreduce.algorithms.statistics.numericstats;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * 각 컬럼의 분위별 데이터 값 목록을 받아 4분위수를 출력함.
 * <알고리즘 상세화 요구됨: 20170906 신홍중>
 * @desc 2nd reducer class for numeric statistics computation mr job (2-step)
 * @version 0.0.1
 * @date : 2013.08.21
 * @author Moonie
 */
public class NumericStats2_QuartilesReducer extends Reducer<Text, Text, NullWritable, Text>{

	private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }
    /**
     * 
     * @parameter Text key : 컬럼번호+ "-{1B, 2B, 3B, 4B}"
     * @parameter Iterable<Text> values : 컬럼 값
     * @parameter Context context
     */
    protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
	{
		String keyStr = key.toString();
//	    컬럼 번호 추출.
        int myAttrIndex = Integer.parseInt(keyStr.substring(0, keyStr.indexOf('-')));
//      4분위중의 블럭 번호 추출.
        int myBlockNum = Integer.parseInt(keyStr.substring(keyStr.indexOf('-')+1, keyStr.length()-1));;
//     컬럼에 해당하는 최소, 최대, 데이터 수 확보.
        String blockTokens[] = context.getConfiguration().get(myAttrIndex + "Block").split(",");
        
        /*
         * input:  전체 데이터 갯수로 부터 각 분위에 해당하는 데이터의 위치를 가져온다.
         */
        long cnt = Long.parseLong(blockTokens[3]);
        long qIndexArr[][] = getQIndex(cnt);
        //속성별 각 분위에 속하는 데이터의 갯수를 Job 변수에서 획득함.
        long blockDataCntArr[] = getBlockDataCnt(myAttrIndex, context.getConfiguration());

        // 자신의 구간에서 써야 하는 (인덱스, 해당 인덱스의 사분위 수)를 저장
        // 저장한 목록이 0이 아니면, 정렬 리스트를 만들고, 해당 인덱스를 출력 결과로 반한
       /*
        * myBlockNum : 분위 번호{1,2,3,4}
        * blockDataCntArr : 분위의 데이터 수
        * qIndexArr : 분위별 데이터 시작 인덱스.
        */
        HashMap<Long, Integer> qIndexMap = isMatchQPosition(myBlockNum, blockDataCntArr, qIndexArr);
        if(qIndexMap!=null)
        {
            ArrayList<Double> valueList = new ArrayList<Double>();
            Iterator<Text> iterator = values.iterator();
            int size = 0;
            while (iterator.hasNext())
            {
                double value = Double.parseDouble(iterator.next().toString());
                if(size==0) valueList.add(value);
                if(size > 0)
                {
                    if(value <= valueList.get(0)) valueList.add(0, value);
                    else if(value >= valueList.get(size-1)) valueList.add(value);
                    else
                    {
                        int index = -1;
                        for(int i=0; i<size; i++)
                        {
                            if(value <= valueList.get(i))
                            {
                                index = i;
                                break;
                            }
                        }
                        if(index >= 0) valueList.add(index, value);
                    }
                }
                size = valueList.size();
            }
            Collections.sort(valueList);

            Set<Long> indexSet = qIndexMap.keySet();
            for(long i: indexSet)
            {
            	double b = Math.round(valueList.get((int)i)*100d) / 100d;
                context.write(NullWritable.get(), new Text(myAttrIndex + "-" + qIndexMap.get(i) + "Q" + delimiter
                                + b));
            }
        }
	}
    

	/**
     * 자신의 구간에서 써야 하는 (인덱스, 해당 인덱스의 사분위 수)를 저장
     * 저장한 목록이 0이 아니면,
     * 정렬 리스트를 만들고, 해당 인덱스를 출력 결과로 반한
	 * @parameter int myBlockSeq : 분위 번호 
	 * @parameter long[] blockCntArr : 분위별 데이터 수 
	 * @parameter long[][] qIndexArr   
	 * @return boolean : 성공(true),실폐(false)
	 */
    private HashMap<Long, Integer> isMatchQPosition(int myBlockSeq, long[] blockCntArr, long[][] qIndexArr)
    {
        HashMap<Long, Integer> qIndexMap = new HashMap<Long, Integer>();

        if((myBlockSeq < 1) || (myBlockSeq > 4)) return null;
        else myBlockSeq--;

        long myStartIndex = 0;
        for(int i=0; i<myBlockSeq; i++) myStartIndex += blockCntArr[i];
        long myEndIndex = myStartIndex + blockCntArr[myBlockSeq] - 1;

        for(int i=0; i<qIndexArr.length; i++)
        {
            for(int k=0; k<qIndexArr[i].length; k++)
            {
                if((qIndexArr[i][k] >= 0)
                        && (qIndexArr[i][k] >= myStartIndex)
                        && (qIndexArr[i][k] <= myEndIndex))
                {
                    qIndexMap.put(qIndexArr[i][k] - myStartIndex, i+1);
                }
            }
        }

        if(qIndexMap.size() == 0) return null;
        else return qIndexMap;
    }

    /**
     * 속성별 각 분위에 속하는 데이터의 갯수를 Job 변수에서 획득함.
     * @parameter int attrIndex : 속성 번호
     * @parameter Configuration conf      : Job 변수.      
     * @return long[]  : 각 분위별 데이터 수.
     *
     */
    private long[] getBlockDataCnt(int attrIndex, Configuration conf)
    {
        long retArr[] = new long[4];

        retArr[0] = Long.parseLong(conf.get(attrIndex + "-1B"));
        retArr[1] = Long.parseLong(conf.get(attrIndex + "-2B"));
        retArr[2] = Long.parseLong(conf.get(attrIndex + "-3B"));
        retArr[3] = Long.parseLong(conf.get(attrIndex + "-4B"));

        return retArr;
    }

    /**
     * 전체 데이터에서 각 분위에 해당하는 값의 위치를 계산하여 2차원 배열로 반환.
	 * @parameter long dataCnt : 전체 데이터 
	 * @return
	 *   분위별 위치를 가진 2차원 배열
     */
    private long[][] getQIndex(long dataCnt)
    {
        long[][] retIndex = new long[3][2];
        double q1 = (double)dataCnt * 0.25;
        double q2 = (double)dataCnt * 0.5;
        double q3 = (double)dataCnt * 0.75;
        if((q1-(int)q1) > 0)
        {
            retIndex[0][0] = -1;
            retIndex[0][1] = (int)q1;
        }
        else
        {
            retIndex[0][0] = (int)q1 - 1;
            retIndex[0][1] = (int)q1;
        }
        if((q2-(int)q2) > 0)
        {
            retIndex[1][0] = -1;
            retIndex[1][1] = (int)q2;
        }
        else
        {
            retIndex[1][0] = (int)q2 - 1;
            retIndex[1][1] = (int)q2;
        }
        if((q3-(int)q3) > 0)
        {
            retIndex[2][0] = -1;
            retIndex[2][1] = (int)q3;
        }
        else
        {
            retIndex[2][0] = (int)q3 - 1;
            retIndex[2][1] = (int)q3;
        }
        return retIndex;
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
