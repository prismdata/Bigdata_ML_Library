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
 * TextDoubleTwoPairsWritableComparable
 * @desc a WritableComparable for text and double two pairs.
 *
 * @version 0.0.1
 * @date : 2013.07.01
 * @author Suhyun Jeon
 */
public class TextDoubleTwoPairsWritableComparable implements WritableComparable<TextDoubleTwoPairsWritableComparable> {

	private Text text1;
	private double number1;
	private Text text2;
	private double number2;
		
    /** 
     * Get the value of the text1
     */ 
	public String getText1() {
		return text1.toString();
	}

    /** 
     * Get the value of the number1
     */ 
	public double getNumber1() {
		return number1;
	}

    /** 
     * Get the value of the text2
     */ 
	public String getText2() {
		return text2.toString();
	}
	
    /** 
     * Get the value of the number2
     */ 
	public double getNumber2() {
		return number2;
	}

    /** 
     * Set the value of the text1
     */ 	
	public void setText1(Text text1) {
		this.text1 = text1;
	}
	
    /** 
     * Set the value of the number2
     */ 
	public void setNumber1(double number1) {
		this.number1 = number1;
	}
	
    /** 
     * Set the value of the text2
     */ 
	public void setText2(Text text2) {
		this.text2 = text2;
	}
	
    /** 
     * Set the value of the number2
     */ 
	public void setNumber2(double number2) {
		this.number2 = number2;
	}

	public TextDoubleTwoPairsWritableComparable()	{
		text1 = new Text("");
		number1 = 0.0d;
		
		text2 = new Text("");
		number2 = 0.0d;
	}
			
	public TextDoubleTwoPairsWritableComparable(String text1, double number1, String text2, double number2) {
		this.text1 = new Text(text1);
		this.number1 = number1;		
		this.text2 = new Text(text2);
		this.number2 = number2;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		text1.readFields(dataInput);
		number1 = dataInput.readDouble();		
		text2.readFields(dataInput);
		number2 = dataInput.readDouble();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		text1.write(dataOutput);
		dataOutput.writeDouble(number1);		
		text2.write(dataOutput);
		dataOutput.writeDouble(number2);
	}
	
    /**
     * Returns the value of the TextDoubleTwoPairsWritableComparable
     */	
	@Override
	public String toString() {
		 return text1.toString() + "\t" + number1 + "\t" + number2;
	}

    @Override
    public int compareTo(TextDoubleTwoPairsWritableComparable textDoubleTwoPairsWritableComparable) {
        return 0;
    }
}
