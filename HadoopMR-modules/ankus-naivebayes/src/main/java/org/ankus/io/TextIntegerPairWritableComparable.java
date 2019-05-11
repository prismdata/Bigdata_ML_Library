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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
/**
 * TextIntegerPairWritableComparable
 * @desc a WritableComparable for text and integer two pairs.
 *
 * @version 0.0.1
 * @date : 2013.07.01
 * @author Suhyun Jeon
 */
public class TextIntegerPairWritableComparable implements WritableComparable<TextIntegerPairWritableComparable>
{
    private Integer number;
    private Text text;

    /** 
     * Get the value of the number
     */
    public Integer getNumber()
    {
        return number;
    }

    /** 
     * Get the value of the number
     */
    public Text getText() {
        return text;
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
    public void setNumber(Integer number) {
        this.number = number;
    }

    public void setTextIntegerPairWritableComparable(Text text, Integer number) {
        this.text = text;
        this.number = number;
    }

    public TextIntegerPairWritableComparable() {
        setTextIntegerPairWritableComparable(new Text(""), new Integer(0));
    }

    public TextIntegerPairWritableComparable(String text, Integer number) {
        setTextIntegerPairWritableComparable(new Text(text), new Integer(number.intValue()));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        this.text.readFields(dataInput);
        this.number = Integer.valueOf(dataInput.readInt());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        this.text.write(dataOutput);
        dataOutput.writeInt(this.number.intValue());
    }

    /**
     * Returns the value of the TextIntegerPairWritableComparable
     */
    @Override
    public String toString()
    {
        return text.toString() + "\t" + number;
    }

    @Override
    public int compareTo(TextIntegerPairWritableComparable textIntegerPairWritableComparable) {
        return 0;
    }
}