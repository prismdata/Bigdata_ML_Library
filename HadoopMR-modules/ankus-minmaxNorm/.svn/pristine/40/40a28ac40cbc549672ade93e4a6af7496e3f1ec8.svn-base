최대/최소 정규화 데이터 통계 분석 모듈
수행 명령
hadoop jar ankus-core2-minmaxNorm-1.1.0.jar \
Normalization \
-input [입력 파일 또는 입력 폴더] \
-output [출력 폴더 경로] \
-delimiter [컬럼 구분자]  \
 -indexList  [수치 데이터 인덱스]
 
출력 경로 내부의 part-r-00000가 자동 생성됨.

예제 
hadoop jar ankus-core2-minmaxNorm-1.1.0.jar \ 
Normalization \
-input /data/minmax_norm.csv \
-output /result/minmax_norm \ 
-delimiter , \
-indexList 0,1,2,3

입력 예제:
..........
5.1,3.8,1.6,0.2,setosa
4.6,3.2,1.4,0.2,setosa
5.3,3.7,1.5,0.2,setosa
5,3.3,1.4,0.2,setosa
7,3.2,4.7,1.4,versicolor
6.4,3.2,4.5,1.5,versicolor
6.9,3.1,4.9,1.5,versicolor
5.5,2.3,4,1.3,versicolor
6.5,2.8,4.6,1.5,versicolor
5.7,2.8,4.5,1.3,versicolor
6.3,3.3,4.7,1.6,versicolor
4.9,2.4,3.3,1,versicolor
6.6,2.9,4.6,1.3,versicolor
5.2,2.7,3.9,1.4,versicolor
5,2,3.5,1,versicolor
5.9,3,4.2,1.5,versicolor
6,2.2,4,1,versicolor
6.1,2.9,4.7,1.4,versicolor
5.6,2.9,3.6,1.3,versicolor
6.7,3.1,4.4,1.4,versicolor
..........
출력 예제
.........
0.222,0.75,0.102,0.042,setosa
0.083,0.5,0.068,0.042,setosa
0.278,0.708,0.085,0.042,setosa
0.194,0.542,0.068,0.042,setosa
0.75,0.5,0.627,0.542,versicolor
0.583,0.5,0.593,0.583,versicolor
0.722,0.458,0.661,0.583,versicolor
0.333,0.125,0.508,0.5,versicolor
0.611,0.333,0.61,0.583,versicolor
0.389,0.333,0.593,0.5,versicolor
0.556,0.542,0.627,0.625,versicolor
0.167,0.167,0.39,0.375,versicolor
0.639,0.375,0.61,0.5,versicolor
0.25,0.292,0.492,0.542,versicolor
0.194,0.0,0.424,0.375,versicolor
0.444,0.417,0.542,0.583,versicolor
0.472,0.083,0.508,0.375,versicolor
0.5,0.375,0.627,0.542,versicolor
0.361,0.375,0.441,0.5,versicolor
0.667,0.458,0.576,0.542,versicolor
............