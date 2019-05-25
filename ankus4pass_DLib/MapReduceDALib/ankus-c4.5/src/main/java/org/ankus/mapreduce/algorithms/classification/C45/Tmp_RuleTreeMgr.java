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

package org.ankus.mapreduce.algorithms.classification.C45;

/**
 * RuleTreeMgr
 * @desc
 *
 * @version 0.1
 * @date : 2013.11.13
 * @author Moonie Song
 */
public class Tmp_RuleTreeMgr {

    private String levelString = "|";
    private String infoDelimiter = "\t";
    private String valueDelimiter = "=";


    private String[] classLabelArr = null;

    public Tmp_RuleTree loadTree(String[] readLines)
    {
        /**
         *  load tree from readlines
         *   - first line is class info
          */

        return null;
    }

    public void setClassLabelArr(String classListStr)
    {
        classLabelArr = classListStr.split(",");
    }

    public void add(Tmp_RuleTree parent, Tmp_RuleTree child)
    {
        if(parent.getLeftChild() == null) parent.setLeftChild(child);
        else
        {
            Tmp_RuleTree temp = parent.getLeftChild();
            while(temp.getRightSibling() != null) temp = temp.getRightSibling();
            temp.setRightSibling(child);
        }
    }

    public Tmp_RuleTree[] getLeafNodes(Tmp_RuleTree parentNode)
    {

        return null;
    }


    public String getCurrentNodeInfoStr(Tmp_RuleTree node)
    {

        String printStr = node.getName() + valueDelimiter + node.getValue();

        if(node.getLeftChild() != null)
        {
            printStr += infoDelimiter + classLabelArr[node.getClassIndex()];

            int[] dataCntArr = node.getDataCntArr();
            /**
             * dataCntArr -> toString
             */



            return printStr;
        }
        else return printStr;
    }





    /**
     * Reference Codes..
     */
    public void printLevelNodes(Tmp_RuleTree node, int level)
    {
        int depth = 0;
        Tmp_RuleTree tempChild = node;
        Tmp_RuleTree tempParent = node;

        while(depth <= level)
        {
            if(depth == level)
            {
                while(tempChild != null)
                {
                    System.out.print(getCurrentNodeInfoStr(tempChild));
                    tempChild = tempChild.getRightSibling();
                }

                if(tempParent.getRightSibling() != null)
                {
                    tempParent = tempParent.getRightSibling();
                    tempChild = tempParent.getLeftChild();
                } else break;
            }
            else
            {
                tempParent = tempChild;
                tempChild = tempChild.getLeftChild();
                depth++;
            }
        }
    }

    public void printChildrenNodes(Tmp_RuleTree node, int depth)
    {
        for(int i = 0; i < depth; i++)
            if(node.getLeftChild() != null)
                printChildrenNodes(node.getLeftChild(), depth + 1);

            if(node.getRightSibling() != null)
                printChildrenNodes(node.getRightSibling(), depth);
    }


}
