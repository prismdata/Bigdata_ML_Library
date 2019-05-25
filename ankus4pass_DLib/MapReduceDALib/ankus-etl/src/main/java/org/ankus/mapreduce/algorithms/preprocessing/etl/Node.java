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

package org.ankus.mapreduce.algorithms.preprocessing.etl;

/**
 * 연결 리스트의 노드를 표현하는 클래스
 * @author HongJoong.Shin
 * @date   2016.12.06
 */
public class Node {
    private String data;
    private Node nextNode;
    /**
     * Node를 생성하면서 값을 설정.
     * @author HongJoong.Shin
     * @date   2016.12.06
     */
    public Node(String data) {
        this.data = data;
    }
    /**
     * Node를 값을 리턴함.
     * @author HongJoong.Shin
     * @date   2016.12.06
     * @return String 노드 값.
     */
    public String getData() {
        return data;
    }
    /**
     * 다음 Node를 연결함.
     * @author HongJoong.Shin
     * @date   2016.12.06
     * @return 없음
     */
    public void setNextNode(Node nextNode) {
        this.nextNode = nextNode;
    }
    /**
     * 다음 Node를 가져옴.
     * @author HongJoong.Shin
     * @date   2016.12.06
     * @return Node 타입의 다음 노드.
     */
    public Node getNextNode() { 
        return nextNode;
    }
}
