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

package org.ankus.mapreduce.algorithms.recommendation.recommender.commons;

/**
 *추천 결과를 객체로 저장하기 위한 클래스.
 */
public class RecomResultStructure {//implements Comparable<RecomResultStructure> {

    private String itemName = "NULL";
    private double recomValue = 0.0;
    private int consideredCount = 0;

    /*
     * 아이템 이름 반환 
     * @param conf 하둡 환경 변수.
     */
    public String getItemName() {
        return itemName;
    }
    
 
    /**
     * 아이템 이름 설정
     * @parameter String itemName 아이템 이름.
     * @return
     */
    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    /**
     * 추천 값 반환.
     * @parameter             
     * @return double 추천값. 
     */
    public double getRecomValue() {
        return recomValue;
    }
    /*
     * 아이템 추천 점수 설정 
     * @param String recomValue 추천 점수.
     */    
    public void setRecomValue(double recomValue) {
        this.recomValue = recomValue;
    }

    /**
     * 평가에 사용된 사용자 수 반환.
     * @parameter 
     * @return int 사용자 수
     */
    public int getConsideredCount() {
        return consideredCount;
    }
    /**
     * 평가에 사용된 사용자 수 설정.
     * @parameter   int consideredCount
     * @return 
     */
    public void setConsideredCount(int consideredCount) {
        this.consideredCount = consideredCount;
    }
       
    /**
     * 추천할 아이템, 추천 점수, 사용자 수 설정.
     * @param String name 사용자 id 
     * @param double value 추천 
     * @param int cnt 추천에 사용된 사용자 수 
     */
    public RecomResultStructure(String name, double value, int cnt)
    {
        this.setItemName(name);
        this.setRecomValue(value);
        this.setConsideredCount(cnt);
    }
   
    /**
     * RecomResultStructure의  아이템, 추천 값, 사용자수를 구분자로 분리하여 문자열로 반환함.
     * @parameter String delimiter 각 항목의 구분자             
     * @return String 추천 결과 값.
     */
    public String toString(String delimiter)
    {
        return this.getItemName() + delimiter + this.getRecomValue() + delimiter + this.getConsideredCount();
    }
    /**
     * 추천 결과를 정렬하기 위해 각 추천 값을 비교한다.
     * @parameter  RecomResultStructure compObj : 추천 아이템 객체.
     * @return
     */
    public int compareTo(RecomResultStructure compObj)
    {
        double value = compObj.getRecomValue() - this.getRecomValue();

    	/* For Decending order*/
        if(value >= 0) return 1;
        else return -1;
    }

}