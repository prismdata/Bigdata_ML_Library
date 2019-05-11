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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * TextTwoWritableComparable
 * @desc a WritableComparable for two texts.
 *
 * @version 0.0.1
 * @date : 2013.07.01
 * @author Suhyun Jeon
 */
public class TextTwoWritableComparable implements WritableComparable<TextTwoWritableComparable> {

	private Text text1;
	private Text text2; 
	
    /** 
     * Get the value of the text1
     */
	public Text getText1() {
		return text1;
	}
	
    /** 
     * Get the value of the text2
     */
	public Text getText2() {
		return text2;
	}
	
    /** 
     * Set the value of the text1
     */   
	public void setText1(Text text1) {
		this.text1 = text1;
	}

    /** 
     * Set the value of the text2
     */   
	public void setText2(Text text2) {
		this.text2 = text2;
	}

	public void setTextTwoWritable(Text text1, Text text2) {
		this.text1 = text1;
		this.text2 = text2;
	}

    public TextTwoWritableComparable() {
		setTextTwoWritable(new Text(""), new Text(""));
	}
	
	public TextTwoWritableComparable(String text1, String text2) {
		setTextTwoWritable(new Text(text1), new Text(text2));
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		text1.readFields(dataInput);
		text2.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		text1.write(dataOutput);
		text2.write(dataOutput);
	}
	
	@Override
	public String toString() {
		 return text1.toString() + "\t" + text2.toString();
	}

    @Override
    public int compareTo(TextTwoWritableComparable textTwoWritableComparable) {
        int compareToParam = text1.compareTo(textTwoWritableComparable.text1);
        if(compareToParam != 0) {
            return compareToParam;
        }
        return text2.compareTo(textTwoWritableComparable.text2);
    }
}
