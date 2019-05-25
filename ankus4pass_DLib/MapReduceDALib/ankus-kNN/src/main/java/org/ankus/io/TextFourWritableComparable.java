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
 * TextFourWritableComparable
 * @desc a WritableComparable for four text.
 *
 * @version 0.0.1
 * @date : 2013.07.01
 * @author Suhyun Jeon
 */
public class TextFourWritableComparable implements WritableComparable<TextFourWritableComparable> {

	private Text text1;
	private Text text2;
	private Text text3;
	private Text text4;
	
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
     * Get the value of the text3
     */ 
	public Text getText3() {
		return text3;
	}

    /** 
     * Get the value of the text4
     */ 
	public Text getText4() {
		return text4;
	}

	public void setTextFourWritableComparable(Text text1, Text text2, Text text3, Text text4) {
		this.text1 = text1;
		this.text2 = text2;
		this.text3 = text3;
		this.text4 = text4;
	}	
	
	public void setText1(Text text1) {
		this.text1 = text1;
	}

	public void setText2(Text text2) {
		this.text2 = text2;
	}

	public void setText3(Text text3) {
		this.text3 = text3;
	}

	public void setText4(Text text4) {
		this.text4 = text4;
	}

	public TextFourWritableComparable() {
		setTextFourWritableComparable(new Text(""), new Text(""), new Text(""), new Text(""));
	}
	
	public TextFourWritableComparable(String text1, String text2, String text3, String text4) {
		setTextFourWritableComparable(new Text(text1), new Text(text2), new Text(text3), new Text(text4));
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		text1.readFields(dataInput);
		text2.readFields(dataInput);
		text3.readFields(dataInput);
		text4.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		text1.write(dataOutput);
		text2.write(dataOutput);
		text3.write(dataOutput);
		text4.write(dataOutput);
	}
	
    /**
     * Returns the value of the TextFourWritableComparable
     */	
	@Override
	public String toString() {
		 return text1.toString() + "\t" + text2.toString() + "\t" + text4.toString();
	}

    @Override
    public int compareTo(TextFourWritableComparable textFourWritableComparable) {
        return 0;
    }
}
