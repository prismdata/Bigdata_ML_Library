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

import org.ankus.util.ArgumentsConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 각 폴드(Reducer)에 대한 컬럼 번호와,폴더별 확신도를 획득하여 Reducer로 출력한다.
 * @author Moonie
 * @version 0.0.1
 * @date : 2013.08.20
 */
public class CFSum2MRMergeMapper extends Mapper<Object, Text, Text, Text>{

	private String delimiter;
	/**
	 * Job을 설정 변수로 부터 구분자를 획득한다.
	 * @author Moonie
	 * @date 2013.08.20
	 * @param Context context : Job을 설정하는 변수 
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // TODO '\t'을 변수명으로 수정해야 함
        delimiter = context.getConfiguration().get(ArgumentsConstants.DELIMITER, "\t");
    }
    /**
	 * Job을 설정 변수로 부터 구분자를 획득한다.
	 * @author Moonie
	 * @date 2013.08.20
	 * @param Object key : 입력 스트릿 오프셋
	 * @param Text value : 입력 스프릿 
	 * @param Context context : Job을 설정하는 변수 
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String valueStr = value.toString();
		int splitIndex = valueStr.indexOf(delimiter);
		
		String keyStr = valueStr.substring(0, splitIndex);
		keyStr = keyStr.substring(0, keyStr.indexOf("_"));
		
		valueStr = valueStr.substring(splitIndex + 1);
		context.write(new Text(keyStr), new Text(valueStr));
	}

}
