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
 * 추천 결과가 저장될 클래스
 * @author: Wonmoon
 * @date: 2015.01.21
 */
public class RecomResultStructure {//implements Comparable<RecomResultStructure> {

    private String itemName = "NULL";
    private double recomValue = 0.0;
    private int consideredCount = 0;

    /**
     * 아이템 아이디를 반환
     * @author Wonmoon
     * @param
     * @return String : 아이템 이름
     */
    public String getItemName() {
        return itemName;
    }

    /**
     * 아이템 아이디를 설정
     * @author  Wonmoon
     * @param String itemName: 아이템 아이디 
     * @return void
     */
    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    /**
     * 추천 값을 반환
     * @author Wonmoon
     * @param
     * @return double  추천값
     */
    public double getRecomValue() {
        return recomValue;
    }
    /**
     * 추천 값을 설정
     * @author Wonmoon
     * @param double recomValue :추천 값
     * @return void
     */
    public void setRecomValue(double recomValue) {
        this.recomValue = recomValue;
    }

    /**
     * 추천에 사용된 아이템의 갯수를 반환
     * @author Wonmoon
     * @param
     * @return int :아이템 갯수
     */
    public int getConsideredCount() {
        return consideredCount;
    }

    /**
     * 추천에 사용된 아이템의 갯수를 설정
     * @author Wonmoon
     * @param int consideredCount: 아이템 갯수
     * @return void
     */
    public void setConsideredCount(int consideredCount) {
        this.consideredCount = consideredCount;
    }

    /**
     * 추천 아이템에 대한 정보를 설정
     * @author Wonmoon
     * @param String name : 아이템 아이디
     * @param double value : 아이템 값
     * @param int cnt : 아이템 갯수
     */
    public RecomResultStructure(String name, double value, int cnt)
    {
        this.setItemName(name);
        this.setRecomValue(value);
        this.setConsideredCount(cnt);
    }

    /**
     * 추천 아이템의 정보를 문자열로 출력
     * @author Wonmoon
     * @param String delimiter : 구분자
     * @return String: 추천 정보
     */
    public String toString(String delimiter)
    {
        return this.getItemName() + delimiter + this.getRecomValue() + delimiter + this.getConsideredCount();
    }

    /**
     *추천 결과를 정렬하기 위해 추천 값을 기준으로 크고 작음을 비교함. 
     * @author Wonmoon
     * @param RecomResultStructure compObj  : 추천 결과 객체
     * @return int :1,0
     */
    public int compareTo(RecomResultStructure compObj)
    {
        double value = compObj.getRecomValue() - this.getRecomValue();

    	/* For Decending order*/
        if(value >= 0) return 1;
        else return -1;
    }

}
