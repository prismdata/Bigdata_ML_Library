컬럼간의 상관성을 계산하는 알고리즘

수행 방법:
hadoop jar  ankus-core2-column-sim-1.1.0.jar  ColumnCorrelation \
-input [입력 데이터 파일 경로] \
-output [출력 데이터 폴더 경로] \
-delimiter [속성 구분자] \
-indexList [계산할 속성의 인덱스] \
-algorithmOption [유사도/거리 계산 옵션 {hamming, dice, jaccard, tanimoto, manhatan, uclidean, cosine, pearson, matching, edit}]

수행 예제:
hadoop jar  ankus-core2-column-sim-1.1.0.jar ColumnCorrelation \
-input /data/column_sim.csv \
-output /result/column_similarity \
-delimiter , \
-indexList 0,1,2 \
-algorithmOption hamming

*입력 예제:
.....
4.7,3.2,1.3,.2,"Setosa"
4.6,3.1,1.5,.2,"Setosa"
5,3.6,1.4,.2,"Setosa"
5.4,3.9,1.7,.4,"Setosa"
4.6,3.4,1.4,.3,"Setosa"
5,3.4,1.5,.2,"Setosa"
4.4,2.9,1.4,.2,"Setosa"
4.9,3.1,1.5,.1,"Setosa"
5.4,3.7,1.5,.2,"Setosa"
4.8,3.4,1.6,.2,"Setosa"
4.8,3,1.4,.1,"Setosa"
4.3,3,1.1,.1,"Setosa"
5.8,4,1.2,.2,"Setosa"
5.7,4.4,1.5,.4,"Setosa"
5.4,3.9,1.3,.4,"Setosa"
5.1,3.5,1.4,.3,"Setosa"
....

*출력 예제:
**/result/column_similarity/part-r-00000 : 각 변수간의 해밍 거리 출력.
[변수, 변수, 거리, 유사도 이름]
0,1,2.0,hamming
0,2,2.0,hamming
1,2,2.0,hamming