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
 * RuleTree
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.13
 * @author Moonie Song
 */
public class Tmp_RuleTree {

    /*

    tear-prod-rate = reduced: none (12.0)
    tear-prod-rate = normal
    |   astigmatism = no: soft (6.0/1.0)
    |   astigmatism = yes
    |   |   spectacle-prescrip = myope: hard (3.0)
    |   |   spectacle-prescrip = hypermetrope: none (3.0/1.0)


    depth, attr-index, attr-value, class(or NONE), total-data-cnt, max-data-cnt

     */

    /*
        outlook = sunny
        |  humidity = high: no
        |  humidity = normal: yes
        outlook = overcast: yes
        outlook = rainy
        |  windy = TRUE: no
        |  windy = FALSE: yes

        < level, attr-index@@attr-value, total-data, max-data, class, isLeafNode >
        outlook@@sunny,5,3,no,false
        outlook@@overcast,4,4,yes,true
        outlook@@rainy,5,3,yes,false
     */


    private String name = null;             // attribute type
    private String value = null;            // attribute value
    private Tmp_RuleTree leftChild = null;      // child node
    private Tmp_RuleTree rightSibling = null;   // sibling node
    private int[] dataCntArr = null;        // data cnt belong to this node (only leaf node)
    private int classIndex = 0;             // class index based on data cnt
    private int totalDataCnt = 0;           // total data cnt
    private int maxDataCnt = 0;             // max data(class) cnt

    public Tmp_RuleTree() {
    }

    public void setNodeInfo(String name, String value) {
        this.name = name;
        this.value = value;
        this.classIndex = -1;
    }

    public void setNodeInfo(String name, String value, int[] dataCntArr) {
        this.name = name;
        this.value = value;
        this.dataCntArr = new int[dataCntArr.length];
        for(int i=0; i<dataCntArr.length; i++) this.dataCntArr[i] = dataCntArr[i];
        setClassIndexAndCntInfo();
    }

    public void setClassIndexAndCntInfo(){
        classIndex = -1;
        maxDataCnt = 0;
        totalDataCnt = 0;

        for(int i=0; i<dataCntArr.length; i++)
        {
            totalDataCnt += dataCntArr[i];
            if(dataCntArr[i] > maxDataCnt)
            {
                maxDataCnt = dataCntArr[i];
                classIndex = i;
            }
        }
    }


    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public int[] getDataCntArr() {
        return dataCntArr;
    }

    public int getMaxDataCnt() {
        return maxDataCnt;
    }

    public int getTotalDataCnt() {
        return totalDataCnt;
    }

    public int getClassIndex() {
        return classIndex;
    }

    public void setLeftChild(Tmp_RuleTree leftChild) {
        this.leftChild = leftChild;
    }

    public Tmp_RuleTree getLeftChild() {
        return leftChild;
    }

    public void setRightSibling(Tmp_RuleTree rightSibling) {
        this.rightSibling = rightSibling;
    }

    public Tmp_RuleTree getRightSibling() {
        return rightSibling;
    }
}
