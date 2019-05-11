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
 * TextIntegerTwoPairsWritableComparable
 * @desc a WritableComparable for text and integer two pairs.
 *
 * @version 0.0.1
 * @date : 2013.07.01
 * @author Suhyun Jeon
 */
public class TextIntegerTwoPairsWritableComparable implements WritableComparable<TextIntegerTwoPairsWritableComparable>

{
    private Text text1;
    private int number1;
    private Text text2;
    private int number2;

    /** 
     * Get the value of the text1
     */
    public Text getText1()
    {
        return this.text1;
    }

    /** 
     * Get the value of the number1
     */
    public int getNumber1() {
        return this.number1;
    }

    /** 
     * Get the value of the text2
     */
    public Text getText2() {
        return this.text2;
    }

    /** 
     * Get the value of the number2
     */
    public int getNumber2() {
        return this.number2;
    }
   
    /** 
     * Set the value of the text1
     */
    public void setText1(Text text1) {
        this.text1 = text1;
    }
    
    /** 
     * Set the value of the number1
     */
    public void setNumber1(int number1) {
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
    public void setNumber2(int number2) {
        this.number2 = number2;
    }
 
    public TextIntegerTwoPairsWritableComparable() {
        this.text1 = new Text("");
        this.number1 = 0;

        this.text2 = new Text("");
        this.number2 = 0;
    }

    public TextIntegerTwoPairsWritableComparable(String text1, int number1, String text2, int number2) {
        this.text1 = new Text(text1);
        this.number1 = number1;

        this.text2 = new Text(text2);
        this.number2 = number2;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        this.text1.readFields(dataInput);
        this.number1 = dataInput.readInt();

        this.text2.readFields(dataInput);
        this.number2 = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        this.text1.write(dataOutput);
        dataOutput.writeInt(this.number1);

        this.text2.write(dataOutput);
        dataOutput.writeInt(this.number2);
    }

    /**
     * Returns the value of the TextIntegerTwoPairsWritableComparable
     */
    @Override
    public String toString()
    {
        return this.text1.toString() + "\t" + this.number1 + "\t" + this.number2;
    }

    @Override
    public int compareTo(TextIntegerTwoPairsWritableComparable textIntegerTwoPairsWritableComparable) {
        return 0;
    }
}