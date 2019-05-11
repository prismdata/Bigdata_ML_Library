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
 * TextDoublePairWritableComparable
 * @desc a WritableComparable for text and double pair.
 *
 * @version 0.0.1
 * @date : 2013.07.01
 * @author Suhyun Jeon
 */
public class TextDoublePairWritableComparable implements WritableComparable<TextDoublePairWritableComparable> {
 
	private Text text;
	private double number;

    /** 
     * Get the value of the text
     */ 
	public String getText() {
		return text.toString();
	}
	
    /** 
     * Get the value of the number
     */ 
	public double getNumber() {
		return number;
	}
	
    /** 
     * Set the value of the text
     */ 
	public void setText(Text text) {
		this.text = text;
	}

    /** 
     * Set the value of the number
     */ 
	public void setNumber(double number) {
		this.number = number;
	}

	public void setMovielensWritableComparable(String text, double number) {
		this.text = new Text(text);
		this.number = number;
	}	
	
	public TextDoublePairWritableComparable(){
		setMovielensWritableComparable("", 0.0d);
	}
	
	public TextDoublePairWritableComparable(String text, double number) {
		setMovielensWritableComparable(text, number);
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		text.readFields(dataInput);
		number = dataInput.readDouble();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		text.write(dataOutput);
		dataOutput.writeDouble(number);
	}

    /**
     * Returns the value of the TextDoublePairWritableComparable
     */	
	@Override
	public String toString() {
		 return text.toString() + "\t" + number;
	}

    @Override
    public int compareTo(TextDoublePairWritableComparable textDoublePairWritableComparable) {
        return 0;
    }
}
