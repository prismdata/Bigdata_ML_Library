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

package org.ankus.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;import java.lang.Integer;import java.lang.Override;import java.lang.String;

/**
 * IntegerTwoWritableComparable
 * @desc a WritableComparable for two integer.
 *
 * @version 0.1.5
 * @date : 2014.06.05
 * @author Suhyun Jeon
 */
public class IntegerTwoWritableComparable implements WritableComparable<IntegerTwoWritableComparable> {

	private Integer integer1;
	private Integer integer2;

    /**
     * Get the value of the text1
     */
	public Integer getInteger1() {
		return integer1;
	}

    /**
     * Get the value of the text2
     */
	public Integer getInteger2() {
		return integer2;
	}

    /**
     * Set the value of the text1
     */
	public void setInteger1(Integer integer1) {
		this.integer1 = integer1;
	}

    /**
     * Set the value of the text2
     */
	public void setInteger2(Integer integer2) {
		this.integer2 = integer2;
	}

	public void setIntegerTwoWritable(Integer integer1, Integer integer2) {
		this.integer1 = integer1;
		this.integer2 = integer2;
	}

    public IntegerTwoWritableComparable() {
        setIntegerTwoWritable(new Integer(0), new Integer(0));
	}

	public IntegerTwoWritableComparable(Integer integer1, Integer integer2) {
        setIntegerTwoWritable(new Integer(integer1), new Integer(integer2));
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
        integer1 = dataInput.readInt();
        integer2 = dataInput.readInt();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(integer1);
        dataOutput.writeInt(integer2);
	}
	
	@Override
	public String toString() {
		 return integer1.toString() + "\t" + integer2.toString();
	}

    @Override
    public int compareTo(IntegerTwoWritableComparable integerTwoWritableComparable) {
        int compareToParam = integer1.compareTo(integerTwoWritableComparable.integer1);
        if(compareToParam != 0) {
            return compareToParam;
        }
        return integer2.compareTo(integerTwoWritableComparable.integer2);
    }
}
