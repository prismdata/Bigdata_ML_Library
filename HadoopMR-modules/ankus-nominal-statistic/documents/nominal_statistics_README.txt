범주 데이터 통계 분석 모듈

수행 방법:
hadoop jar ankus-core2-nominal-statistic-1.1.0.jar \
NominalStatistics \
-input [입력 파일]  \
-output [출력 경로] \
-delimiter [구분자] \
-indexList [범주 데이터 인덱스(1개만 수용)] \
-tempDelete [임시 파일 삭제 옵션(true, false)\

[출력 경로]/result가 자동 생성 

임시 파일 경로
[출력 폴더 경로]nomainal_statistic_freqs

수행 예제: 
hadoop jar ankus-core2-nominal-statistic-1.1.0.jar \
NominalStatistics \
-input /data/nominal_statistic.csv \
-output /result/nomainal_statistic \
-delimiter , \
-indexList 4 \
-tempDelete true

*입력 예제
........
5.1,2.5,3,1.1,versicolor
5.7,2.8,4.1,1.3,versicolor
6.3,3.3,6,2.5,virginica
5.8,2.7,5.1,1.9,virginica
7.1,3,5.9,2.1,virginica
6.3,2.9,5.6,1.8,virginica
6.5,3,5.8,2.2,virginica
7.6,3,6.6,2.1,virginica
4.9,2.5,4.5,1.7,virginica
7.3,2.9,6.3,1.8,virginica
6.7,2.5,5.8,1.8,virginica
7.2,3.6,6.1,2.5,virginica
6.5,3.2,5.1,2,virginica
6.4,2.7,5.3,1.9,virginica
6.8,3,5.5,2.1,virginica
.........
*출력 예제
**/result/nomainal_statistic/result : 범주 통계 분석 결과 파일.
# Attr-4,frequency,ratio
setosa,50,0.3333333333333333
versicolor,50,0.3333333333333333
virginica,50,0.3333333333333333