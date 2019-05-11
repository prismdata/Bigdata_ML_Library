수치/범주형 데이터에 대해 거리에 기반하여 데이터를 그룹화하는 방법
보통 k-means등의 군집 이전에 군집의 개수나 초기 중심을 설정하기 위해 사용됨 

수행 방법:
hadoop jar ankus-core2-canopy-1.1.0.jar canopy
-input [입력 파일 또는 입력 폴더] \
-output [출력 데이터 폴더 경로] \ 
-delimiter [속성 구분자] \
-indexList [수치형 속성 목록] \
-nominalIndexList [범주 데이터 속성 목록, 존재하지 않을 경우 생략] \
-distanceOption [{uclidean|manhattan}] \
-t1 [군집 생성 및 할당 여부를 제한하기 위한 거리 값] \
-t2 [군집 생성 및 할당 여부를 제한하기 위한 거리 값으로 t1값 보다 작아야 함.] \
-finalResultGen [학습 파일에 대한 군집 할당 결과 생성 여부{true|false}]

수행 예제 군집의 중심만 출력함.:
hadoop jar ankus-core2-canopy-1.1.0.jar canopy -input /data/canopy.csv -output /result/canopy -delimiter , -indexList 0,1,2,3 -distanceOption uclidean -t1 0.3 -t2 0.1 -finalResultGen false
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
**/result/canopy/canopy/part-r-00000 : 군집의 중심 출력.
[변수1, 변수2, 변수3, 변수4, 클래스]
5.9,3,5.1,1.8,"Virginica"
7.7,3,6.1,2.3,"Virginica"
5.1,2.5,3,1.1,"Versicolor"
5,3.3,1.4,.2,"Setosa"

수행 예제 테스트 데이터에 군집까지 할당함.:
hadoop jar ankus-core2-canopy-1.1.0.jar canopy -input /data/canopy.csv -output /result/canopy -delimiter , -indexList 0,1,2,3 -distanceOption uclidean -t1 0.3 -t2 0.1 -finalResultGen true
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
**/result/canopy/canopy/part-r-00000 : 군집의 중심 출력.
[변수1, 변수2, 변수3, 변수4, 클래스]
5.9,3,5.1,1.8,"Virginica"
7.7,3,6.1,2.3,"Virginica"
5.1,2.5,3,1.1,"Versicolor"
5,3.3,1.4,.2,"Setosa"

**/result/canopy/CanopyResult/part-m-00000 : 테스트 데이터에 군집 할당
[변수1, 변수2, 변수3, 변수4, 클래스, 중심 여부] 
5.3,3.7,1.5,.2,"Setosa",data
5,3.3,1.4,.2,"Setosa",canopy
7,3.2,4.7,1.4,"Versicolor",data
6.4,3.2,4.5,1.5,"Versicolor",data
6.9,3.1,4.9,1.5,"Versicolor",data
....
7.9,3.8,6.4,2,"Virginica",data
6.4,2.8,5.6,2.2,"Virginica",data
6.3,2.8,5.1,1.5,"Virginica",data
6.1,2.6,5.6,1.4,"Virginica",data
7.7,3,6.1,2.3,"Virginica",canopy
6.3,3.4,5.6,2.4,"Virginica",data
6.4,3.1,5.5,1.8,"Virginica",data
....