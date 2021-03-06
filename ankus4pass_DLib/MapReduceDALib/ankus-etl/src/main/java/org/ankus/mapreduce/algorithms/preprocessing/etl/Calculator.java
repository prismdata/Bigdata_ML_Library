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
 * 규칙 해석 클래스 
 * @author HongJoong.Shin
 * @date 2016.12.06
 */
public class Calculator {
	 	private final char[] NUMBER = { '0', '1'};
	    private final char OPERAND = 'O';
	    private final char LEFT_PARENTHESIS = '(';
	    private final char RIGHT_PARENTHESIS = ')';	       
	    private final char AND = '&';
	    private final char OR = '|';	    
	    private char[] token;
	    private int tokenType;
	    private LinkedListStack stack;
	    /**
	     * 계산 스택을 초기화 함.
	     * @author HongJoong.Shin
	     * @date 2016.12.06
	     */
	    public Calculator() {
	        stack = new LinkedListStack();
	    }
	    /**
	     * 입력 문자가 숫자로 변경 가능한지 검사.
	     * @author HongJoong.Shin
	     * @date 2016.12.06
	     * @param token  검사할 문자
	     * @return 변경 가능한 경우 true, 불가능할 경우 false 
	     */
	    public boolean isNumber(char token) {
	        for (int i = 0; i < NUMBER.length; i++)
	            if (token == NUMBER[i])
	                return true;
	 
	        return false;
	    }
	 
	    /**
	     * 중위표기식에서 토큰을 추출하는 함수(토크나이징)
	     * @author HongJoong.Shin
	     * @date   2016.12.06
	     * @param  infixExpression 중위표기식으로 기술된 논리식.
	     * @param  chrArray 연산또는 피연산자 값을 갖는 변
	     * @return 읽은 문자의 위치
	     */
	    public int getNextToken(String infixExpression, char[] chrArray) {
	        int i = 0;
	        infixExpression += ' ';
	 
	        // null이 나올때까지 반복
	        for (i = 0; infixExpression.charAt(i) != 0; i++) {
	            // 문자를 하나씩 추출한다.
	            chrArray[i] = infixExpression.charAt(i);	            
	            // 피연산자이면 타입을 표시
	            if (isNumber(chrArray[i])) {
	                tokenType = OPERAND;
	                // 만약 피연산자 다음의 문자가 피연산자가 아니라면 중지
	                if (!isNumber(infixExpression.charAt(i + 1)))
	                    break;
	            } else {
	                // 연산자이면 대입한다.
	                tokenType = infixExpression.charAt(i);
	                break;
	            }
	        }	 
	        // 추출된 토큰을 복사한다.
	        token = new char[++i];
	        for (int j = 0; j < i; j++)
	            token[j] = chrArray[j];
	        return i;
	    }
	 
	    /**
	     * 중위 -> 후위표기법 변환 함수
	     * @author HongJoong.Shin
	     * @date   2016.12.06
	     * @param infixExpression: 중위식 표현 문자열.
	     * @return 후위표기법으로 변경된 문자열.
	     */
	    public String getPostfix(String infixExpression) {
	        StringBuffer postfixExpression = new StringBuffer();
	        int position = 0;
	        int length = infixExpression.length();
	        char[] chArr = new char[length];
	        Node popped;
	 
	        // 문자열을 다 읽을때까지 반복
	        while (position < length) {
	            // position 위치부터 토큰을 하나씩 가져온다.
	            position += getNextToken(infixExpression.substring(position), chArr);
	 
	            // 추출된 토큰의 타입이 피연산자라면 출력
	            if (tokenType == OPERAND) {
	                postfixExpression.append(token);
	                postfixExpression.append(' ');
	            } else {
	                // 연산자가 오른쪽 괄호 ')' 라면 스택에서 '('가 나올때까지 제거연산 수행
	                if (tokenType == RIGHT_PARENTHESIS) {
	                    while (!stack.isEmpty()) {
	                        popped = stack.pop();	 
	                        // 제거한 노드가 '(' 라면 중지
	                        if (popped.getData().charAt(0) == LEFT_PARENTHESIS)
	                            break;
	                        else
	                            postfixExpression.append(popped.getData());
	                    }
	                }
	            }
	        }
	 
	        // 스택에 남아 있는 노드들을 제거연산한다.
	        while (!stack.isEmpty()) {
	            popped = stack.pop();	 
	            // '(' 빼고 모두 출력
	            if (popped.getData().charAt(0) != LEFT_PARENTHESIS)
	                postfixExpression.append(popped.getData());
	        }
	 
	        return postfixExpression.toString();
	    }
	 
	    /**
	     * 논리식을 계산함.
	     * @author HongJoong.Shin
	     * @date   2016.12.06
	     * @param postfixExpression : 논리식.
	     * @return 논리식의 연산 결과  1 : True, 0: False
	     */
	    int calculate(String postfixExpression) {
	        int position = 0;
	        int length = postfixExpression.length();
	        char[] chrArr = new char[length];
	        int result = 0;
	        int operand1, operand2;
	        LinkedListStack stack = new LinkedListStack();
	 
	        while (position < length) {
	            position += getNextToken(postfixExpression.substring(position),chrArr);
	 
	            // 공백은 패스
	            if (tokenType == ' ')
	                continue;
	 
	            // 피연산자이면 스택에 삽입
	            if (tokenType == OPERAND) {
	                stack.push(new Node(String.valueOf(token)));
	            } else {
	                // 연산자이면 스택에서 제거연산을 두 번 수행 후
	                operand2 = Integer.parseInt(stack.pop().getData());
	                operand1 = Integer.parseInt(stack.pop().getData());	                
	                // 연산
	                switch (tokenType)
	                {
		                case AND:
		                	result = operand1 * operand2;
		                    break;
		                case OR:
		                	if( operand1 + operand2 >= 1)
		                		{
		                		result = 1;
		                		}
		                	else
		                	{
		                		result = 0;
		                	}
		                    break;
	                }
	                stack.push(new Node(String.valueOf(result)));
	            }
	        }
	        return result;
	    }
}
