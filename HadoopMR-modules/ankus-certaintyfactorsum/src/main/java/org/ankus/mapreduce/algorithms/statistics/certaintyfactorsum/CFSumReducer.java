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

package org.ankus.mapreduce.algorithms.statistics.certaintyfactorsum;

import java.io.IOException;
import java.util.Iterator;

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CFSum2MRSplitMapper으로 부터 Key와 Value를 받아 확신도 합계를 구하는 클래스
 * @version 0.0.1
 * @date : 2013.08.20
 * @author Moonie
 */
public class CFSumReducer extends Reducer<Text, Text, NullWritable, Text>{

    // value for vf sum max
	private double sumMax;
	private String delimiter;
	/**
	 * 확신도 합 최대값과 구분자를 획득한다.
	 * @author Moonie
     * @date 2013.08.20
	 * @param Context context : Job을 설정하는 변수 
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        sumMax = Double.parseDouble(context.getConfiguration().get(ArgumentsConstants.CERTAINTY_FACTOR_MAX, "1"));
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }
    /**
     * key와 Value를 받아 확신도 합계를 산출함
     * @author Moonie
     * @date 2013.08.20
     * @param Text key : 확신도 인덱스
     * @param Iterable<Text> values : 확신도 리스트
     * @param Context context  : Job을 설정하는 변수 
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
	protected void reduce(Text key, Iterable<Text> values, Context context)	throws IOException, InterruptedException
	{
		Iterator<Text> iterator = values.iterator();
		double m_sum = 0;

        while (iterator.hasNext())
        {
            double value = Double.parseDouble(iterator.next().toString());
            m_sum = m_sum + value - (m_sum * value / sumMax);
        }
        context.write(NullWritable.get(), new Text(key.toString() + delimiter + m_sum));
	}
}