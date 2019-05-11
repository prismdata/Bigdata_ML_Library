클래스 분포를 고려한 수치 데이터 이산화 

수행 방법:
hadoop jar ankus-core2-mdl-1.1.0.jar \
EntropyDiscretization \
-input [입력 파일 또는 입력 폴더] \
-output [출력 폴더 경로] \
-delimiter [컬럼 구분자]  \
-indexList [이산화할  수치형 데이터 컬럼 인덱스 리스트] \
-classIndex [클래스 인덱스]


수행 예제:
hadoop jar ankus-core2-mdl-1.1.0.jar \
EntropyDiscretization \
-input /data/mdl.csv \
-output /result/mdl \
-indexList 0 \
-classIndex 4 \
-delimiter ,
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
**/result/mdl/part-r-00000
(5.6~6.2],3,5.1,1.8,"Virginica"
(5.6~6.2],3.4,5.4,2.3,"Virginica"
>6.2,3,5.2,2,"Virginica"
>6.2,2.5,5,1.9,"Virginica"
>6.2,3,5.2,2.3,"Virginica"
>6.2,3.3,5.7,2.5,"Virginica"
>6.2,3.2,5.9,2.3,"Virginica"
(5.6~6.2],2.7,5.1,1.9,"Virginica"
>6.2,3.1,5.1,2.3,"Virginica"
>6.2,3.1,5.6,2.4,"Virginica"
>6.2,3.1,5.4,2.1,"Virginica"
(5.6~6.2],3,4.8,1.8,"Virginica"
>6.2,3.1,5.5,1.8,"Virginica"
>6.2,3.4,5.6,2.4,"Virginica"
>6.2,3,6.1,2.3,"Virginica"
(5.6~6.2],2.6,5.6,1.4,"Virginica"
>6.2,2.8,5.1,1.5,"Virginica"
>6.2,2.8,5.6,2.2,"Virginica"
>6.2,3.8,6.4,2,"Virginica"
>6.2,2.8,6.1,1.9,"Virginica"
>6.2,3,5.8,1.6,"Virginica"
>6.2,2.8,5.6,2.1,"Virginica"
(5.6~6.2],3,4.9,1.8,"Virginica"
(5.6~6.2],2.8,4.8,1.8,"Virginica"
>6.2,3.2,6,1.8,"Virginica"
>6.2,3.3,5.7,2.1,"Virginica"
>6.2,2.7,4.9,1.8,"Virginica"
>6.2,2.8,6.7,2,"Virginica"
<=5.6,2.8,4.9,2,"Virginica"
>6.2,3.2,5.7,2.3,"Virginica"