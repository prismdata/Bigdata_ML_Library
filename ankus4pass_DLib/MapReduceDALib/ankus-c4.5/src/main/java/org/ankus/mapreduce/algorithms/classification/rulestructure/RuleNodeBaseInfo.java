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

package org.ankus.mapreduce.algorithms.classification.rulestructure;

/**
 * RuleNodeBaseInfo
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.13
 * @author Moonie Song
 */
public class RuleNodeBaseInfo {

    public static String conditionDelimiter = "@@";
    public String attrCondition;
    private String attrValue;

    private int dataCnt;
    private double purity;
    private String classLabel;

    boolean isLeaf;

    public RuleNodeBaseInfo()
    {

    }

    public RuleNodeBaseInfo(String attrCondition, String attrValue, int dataCnt, double purity, String classLabel)
    {
        setNodeInfo(attrCondition, attrValue, dataCnt, purity, classLabel);
        setIsLeaf(false);
    }

    public void setNodeInfo(String attrCondition, String attrValue, int dataCnt, double purity, String classLabel)
    {
        this.attrCondition = attrCondition;
        this.attrValue = attrValue;
        this.dataCnt = dataCnt;
        this.purity = purity;
        this.classLabel = classLabel;
    }
    
    public String getattrValue(){
    	return this.attrValue;
    }
    
    public void setIsLeaf(boolean isLeaf)
    {
        this.isLeaf = isLeaf;
    }
    
    

    public String toString(String delimiter)
    {
        String retVal = "";

        String condStr = "";
        int addIndex = this.attrValue.lastIndexOf(this.conditionDelimiter);
        if(addIndex < 0) condStr = this.attrCondition + this.conditionDelimiter + this.attrValue;
        else
        {
            condStr = this.attrValue.substring(0, addIndex)
                    + this.conditionDelimiter + this.attrCondition
                    + this.conditionDelimiter + this.attrValue.substring(addIndex + 2);
        }

        retVal = condStr
                + delimiter + this.dataCnt
                + delimiter + this.purity
                + delimiter + this.classLabel
                + delimiter + this.isLeaf;

        return retVal;
    }
}
