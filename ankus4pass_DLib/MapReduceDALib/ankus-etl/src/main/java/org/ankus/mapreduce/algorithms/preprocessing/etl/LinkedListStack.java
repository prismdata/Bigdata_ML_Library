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
 * 계산 스택을 위한 연결 리스트 형성
 * @author HongJoong.Shin
 * @date   2016.12.06
 */
public class LinkedListStack {
	private Node top;       // tail
    private Node list;      // head
 
    /**
     * 노드를 스택에 추가함.
     * @author HongJoong.Shin
 	 * @date   2016.12.06
     * @param  newNode 추가할 노드 
     */
    public void push(Node newNode) {
        // 스택이 비어있을 경우
        if (list == null)
            list = newNode;
        // 스택이 비어있지 않으면
        else {
            // top(꼬리)을 찾아 연결한다.
            Node currentTop = list;
            while (currentTop.getNextNode() != null)
                currentTop = currentTop.getNextNode();
 
            currentTop.setNextNode(newNode);
        }
        top = newNode;
    }
 
    /**
     * 노드를 스택에서 제거함.
     * @author HongJoong.Shin
 	 * @date   2016.12.06
     * @return  제거된 노드.
     */
    public Node pop() {
        Node popped = top;
 
        // 제거할 노드가 head와 같다면 스택을 비운다.
        if (list == popped) {
            list = null;
            top = null;
        } else {
            // 그렇지 않다면 top을 갱신
            Node currentTop = list;
            while (currentTop.getNextNode() != popped)
                currentTop = currentTop.getNextNode();
 
            top = currentTop;
            top.setNextNode(null);
        }
        return popped;
    }
     
    /**
     * 최상위 노드 반환
     * @author HongJoong.Shin
 	 * @date   2016.12.06
     * @return  최상위 노드.
     */
    public Node getTop() {
        return top;
    }
 
    /**
     * 연결 리스트형 스택의 전체 길이 반환.
     * @author HongJoong.Shin
 	 * @date   2016.12.06
     * @return int 형 전체 길이.
     */
    public int getSize() {
        Node currentTop = list;
        int count = 0;
 
        while (currentTop != null) {
            currentTop = currentTop.getNextNode();
            count++;
        } 
        return count;
    }
 
    /**
     * 노드가 비어있는지 확인
     * @author HongJoong.Shin
 	 * @date   2016.12.06
     * @return boolean : 비어 있으면 True, 아니면 False
     */
    public boolean isEmpty() {
        return list == null;
    }
}
