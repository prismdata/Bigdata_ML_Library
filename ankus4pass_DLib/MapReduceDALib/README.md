#트리 기반 분류 알고리즘

hadoop jar ankus-core2-c4.5-1.1.0.jar \
C45 \
-input [입력 파일 또는 입력 폴더] \
-output [출력 데이터 폴더 경로] \
-delimiter [속성 구분자] \
-indexList [알고리즘에 사용할 데이터 인덱스] \
-exceptionIndexList [알고리즘에서 제외할 데이터 인덱스] \
-classIndex [알고리즘에서 사용할 예측값 가지는 인덱스] \
-minLeafData [단말 노드에서 클래스 종류 수] \
-finalResultGen [학습데이터 분류 결과 생성 여부{true|false}] \
-modelPath [학습된 모델이 있는 경로명]

exceptionIndexList  : 사용하지 않을 경우 생략함.
modelPath : 학습된 모델을 사용할 때만 기술하며 학습시에는 생략함.
finalResultGen: true로 설정 시 모델, 테스트 결과, 성능 결과 파일이 모두 출력됨.
finalResultGen: false로 설정 시 모델만 출력됨.

수행 예제 학습:
hadoop jar ankus-core2-c4.5-1.1.0.jar \
C45 \
-input /data/c4.5.csv \
-output /result/c45_model \
-delimiter , \
-indexList 0,1,2,3 \
-classIndex 4 \
-minLeafData 2 \
-finalResultGen true

*출력 예제:
**/result/c45_model/C45_rule.txt: 트리 모델을 텍스트로 표현한 규칙 파일
 [AttributeName-@@Attribute-Value][@@].., Data-Count, Node-Purity, Class-Label, Is-Leaf-Node
2@@<&&1.9,50,0.0,"Setosa",true
2@@>&&1.9,100,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@<&&1.7,54,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@<&&1.7@@2@@<&&4.9,48,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@<&&1.7@@2@@<&&4.9@@3@@<&&1.6,47,0.0,"Versicolor",true
2@@>&&1.9@@3@@<&&1.7@@2@@<&&4.9@@3@@>&&1.6,1,0.0,"Virginica",true
2@@>&&1.9@@3@@<&&1.7@@2@@>&&4.9,6,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@<&&1.7@@2@@>&&4.9@@3@@<&&1.5,3,0.0,"Virginica",true
2@@>&&1.9@@3@@<&&1.7@@2@@>&&4.9@@3@@>&&1.5,3,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@<&&1.7@@2@@>&&4.9@@3@@>&&1.5@@0@@<&&6.7,2,0.0,"Versicolor",true
2@@>&&1.9@@3@@<&&1.7@@2@@>&&4.9@@3@@>&&1.5@@0@@>&&6.7,1,0.0,"Virginica",true
2@@>&&1.9@@3@@>&&1.7,46,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@>&&1.7@@2@@<&&4.8,3,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@>&&1.7@@2@@<&&4.8@@0@@<&&5.9,1,0.0,"Versicolor",true
2@@>&&1.9@@3@@>&&1.7@@2@@<&&4.8@@0@@>&&5.9,2,0.0,"Virginica",true
2@@>&&1.9@@3@@>&&1.7@@2@@>&&4.8,43,0.0,"Virginica",true

**/result/c45_model/classifying_result/part-m-00000 :생성된 모델을 사용하여 학습에 사용된 입력 데이터를 분류한 결과
학습 데이터로 모델 테스트시에는 학습 데이터 뒤에 분류 결과가 추가 됨.
........
5.5,3.5,1.3,.2,"Setosa","Setosa"
4.9,3.6,1.4,.1,"Setosa","Setosa"
4.4,3,1.3,.2,"Setosa","Setosa"
5.1,3.4,1.5,.2,"Setosa","Setosa"
5,3.5,1.3,.3,"Setosa","Setosa"
4.5,2.3,1.3,.3,"Setosa","Setosa"
4.4,3.2,1.3,.2,"Setosa","Setosa"
5,3.5,1.6,.6,"Setosa","Setosa"
5.1,3.8,1.9,.4,"Setosa","Setosa"
4.8,3,1.4,.3,"Setosa","Setosa"
5.1,3.8,1.6,.2,"Setosa","Setosa"
4.6,3.2,1.4,.2,"Setosa","Setosa"
5.3,3.7,1.5,.2,"Setosa","Setosa"
5,3.3,1.4,.2,"Setosa","Setosa"
7,3.2,4.7,1.4,"Versicolor","Versicolor"
6.4,3.2,4.5,1.5,"Versicolor","Versicolor"
........
**/result/c45_model/validation.txt : 알고리즘 및 Machine Learning 모델의 성능을 평가 결과가 출력됨.
" Total Summary "
Total Instances: 150
Correctly Classified Instances: 148(98.67%)
Incorrectly Classified Instances: 2(1.33%)

" Confusion Matrix"
(Classified as)	"Setosa"	"Versicolor"	"Virginica"	|	total	
"Setosa"	50	0	0	|	"50
"Versicolor"	0	48	2	|	"50
"Virginica"	0	0	50	|	"50
total	50	48	52

 Detailed Accuracy
Class	TP_Rate	FP_Rate	Precision	Recall	F-Measure
"Setosa"	1.000	0.000	1.000	1.000	1.000
"Versicolor"	0.960	0.000	1.000	0.960	0.980
"Virginica"	1.000	0.020	0.962	1.000	0.980
Weig.Avg.	0.987	0.007	0.987	0.987	0.987

수행 예제 모델 테스트:
hadoop jar ankus-core2-c4.5-1.1.0.jar \
C45 \
-input /data/c4.5.csv \
-output /result/c45_test \
-modelPath /result/c45_model/C45_rule.txt \
-delimiter , \
-indexList 0,1,2,3 \
-classIndex 4 \
-minLeafData 2 \
-finalResultGen true

*입력 예제:
....
5.4,3.9,1.3,.4,"Setosa"
5.1,3.5,1.4,.3,"Setosa"
5.7,3.8,1.7,.3,"Setosa"
5.1,3.8,1.5,.3,"Setosa"
5.4,3.4,1.7,.2,"Setosa"
5.1,3.7,1.5,.4,"Setosa"
4.6,3.6,1,.2,"Setosa"
5.1,3.3,1.7,.5,"Setosa"
4.8,3.4,1.9,.2,"Setosa"
5,3,1.6,.2,"Setosa"
5,3.4,1.6,.4,"Setosa"
5.2,3.5,1.5,.2,"Setosa"
5.2,3.4,1.4,.2,"Setosa"
4.7,3.2,1.6,.2,"Setosa"
4.8,3.1,1.6,.2,"Setosa"
5.4,3.4,1.5,.4,"Setosa"
5.2,4.1,1.5,.1,"Setosa"
5.5,4.2,1.4,.2,"Setosa"
4.9,3.1,1.5,.2,"Setosa"
5,3.2,1.2,.2,"Setosa"
...
*모델 예제:
**/result/c45_model/C45_rule.txt
" [AttributeName-@@Attribute-Value][@@].., Data-Count, Node-Purity, Class-Label, Is-Leaf-Node
2@@<&&1.9,50,0.0,"Setosa",true
2@@>&&1.9,100,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@<&&1.7,54,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@<&&1.7@@2@@<&&4.9,48,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@<&&1.7@@2@@<&&4.9@@3@@<&&1.6,47,0.0,"Versicolor",true
2@@>&&1.9@@3@@<&&1.7@@2@@<&&4.9@@3@@>&&1.6,1,0.0,"Virginica",true
2@@>&&1.9@@3@@<&&1.7@@2@@>&&4.9,6,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@<&&1.7@@2@@>&&4.9@@3@@<&&1.5,3,0.0,"Virginica",true
2@@>&&1.9@@3@@<&&1.7@@2@@>&&4.9@@3@@>&&1.5,3,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@<&&1.7@@2@@>&&4.9@@3@@>&&1.5@@0@@<&&6.7,2,0.0,"Versicolor",true
2@@>&&1.9@@3@@<&&1.7@@2@@>&&4.9@@3@@>&&1.5@@0@@>&&6.7,1,0.0,"Virginica",true
2@@>&&1.9@@3@@>&&1.7,46,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@>&&1.7@@2@@<&&4.8,3,0.0,"Virginica",false-cont
2@@>&&1.9@@3@@>&&1.7@@2@@<&&4.8@@0@@<&&5.9,1,0.0,"Versicolor",true
2@@>&&1.9@@3@@>&&1.7@@2@@<&&4.8@@0@@>&&5.9,2,0.0,"Virginica",true
2@@>&&1.9@@3@@>&&1.7@@2@@>&&4.8,43,0.0,"Virginica",true
*출력 예제:
**/result/c45_test/classifying_result/part-m-00000
....
5.8,2.7,3.9,1.2,"Versicolor","Versicolor"
6,2.7,5.1,1.6,"Versicolor","Virginica"
5.4,3,4.5,1.5,"Versicolor","Versicolor"
6,3.4,4.5,1.6,"Versicolor","Versicolor"
6.7,3.1,4.7,1.5,"Versicolor","Versicolor"
6.3,2.3,4.4,1.3,"Versicolor","Versicolor"
5.6,3,4.1,1.3,"Versicolor","Versicolor"
5.5,2.5,4,1.3,"Versicolor","Versicolor"
5.5,2.6,4.4,1.2,"Versicolor","Versicolor"
6.1,3,4.6,1.4,"Versicolor","Versicolor"
5.8,2.6,4,1.2,"Versicolor","Versicolor"
5,2.3,3.3,1,"Versicolor","Versicolor"
5.6,2.7,4.2,1.3,"Versicolor","Versicolor"
5.7,3,4.2,1.2,"Versicolor","Versicolor"
5.7,2.9,4.2,1.3,"Versicolor","Versicolor"
6.2,2.9,4.3,1.3,"Versicolor","Versicolor"
5.1,2.5,3,1.1,"Versicolor","Versicolor"
5.7,2.8,4.1,1.3,"Versicolor","Versicolor"
6.3,3.3,6,2.5,"Virginica","Virginica"
5.8,2.7,5.1,1.9,"Virginica","Virginica"
7.1,3,5.9,2.1,"Virginica","Virginica"
....
*검증 예제:
**/result/c45_test/validation.txt
 Total Summary
Total Instances: 150
Correctly Classified Instances: 148(98.67%)
Incorrectly Classified Instances: 2(1.33%)

 Confusion Matrix
(Classified as)	"Setosa"	"Versicolor"	"Virginica"	|	total	
"Setosa"	50	0	0	|	"50
"Versicolor"	0	48	2	|	"50
"Virginica"	0	0	50	|	"50
total	50	48	52

" Detailed Accuracy
Class	TP_Rate	FP_Rate	Precision	Recall	F-Measure
"Setosa"	1.000	0.000	1.000	1.000	1.000
"Versicolor"	0.960	0.000	1.000	0.960	0.980
"Virginica"	1.000	0.020	0.962	1.000	0.980
Weig.Avg.	0.987	0.007	0.987	0.987	0.987

***ETL
수행 방법 모델 생성 :
hadoop ankus-core2-etl-1.1.0.jar \
ETL \
-input [입력 파일 또는 입력 폴더] \
-output [출력 데이터 폴더 경로] \
-delimiter [컬럼 구분자] \
-etlMethod [데이터 변환 기능 선택{ColumnExtractor|FilterInclude|FilterExclude|Replace|NumericNorm|Sort}] \
-indexList [데이터 변환을 수행할 대상 컬럼 리스트] \
-exceptionIndexList [indexList에서 제외할 컬럼 리스트] \
-filterRulePath [규칙이 기술된 파일 경로] \
-filterRule [규칙이 기술된 문자열] \
-Sort [오름 차순, 내림 차순 선택{asc|desc}] \
-SortTarget [정렬 대상 1개 컬럼을 기술] \

-Sort : etlMethod 값이 Sort일때 활성화
-SortTarget : etlMethod 값이 Sort일때 활성화

filterRule은 파일, 문자열 사용 중 하나만 사용함.
-filterRulePath : etlMethod 값이 FilterInclude,FilterExclude,Replace,NumericNorm 일 경우 활성화됨.
-filterRule : etlMethod 값이 FilterInclude,FilterExclude,Replace,NumericNorm 일 경우 활성화됨.

수행 예제1 : 입력 파일에서 특정 컬럼만 추출하는 기능
hadoop jar ankus-core2-etl-1.1.0.jar \
ETL \
-input /data/etl.csv \
-output /result/etl_ColumnExtractor \
-delimiter , \
-etlMethod ColumnExtractor \
-indexList 0,1

*입력 예제
........
5.1,2.5,3,1.1,"Versicolor"
5.7,2.8,4.1,1.3,"Versicolor"
6.3,3.3,6,2.5,"Virginica"
5.8,2.7,5.1,1.9,"Virginica"
7.1,3,5.9,2.1,"Virginica"
6.3,2.9,5.6,1.8,"Virginica"
6.5,3,5.8,2.2,"Virginica"
7.6,3,6.6,2.1,"Virginica"
4.9,2.5,4.5,1.7,"Virginica"
7.3,2.9,6.3,1.8,"Virginica"
6.7,2.5,5.8,1.8,"Virginica"
7.2,3.6,6.1,2.5,"Virginica"
6.5,3.2,5.1,2,"Virginica"
6.4,2.7,5.3,1.9,"Virginica"
6.8,3,5.5,2.1,"Virginica"
5.7,2.5,5,2,"Virginica"
5.8,2.8,5.1,2.4,"Virginica"
6.4,3.2,5.3,2.3,"Virginica"
.........
*출력 예제
**/result/etl_ColumnExtractor/part-r-00000
....
6,2.9
6.7,3
6.8,2.8
6.6,3
6.4,2.9
6.1,2.8
6.3,2.5
6.1,2.8
5.9,3.2
5.6,2.5
6.2,2.2
5.8,2.7
5.6,3
6.7,3.1
....

수행 예제2 : 입력 파일에서 특정 컬럼에서 특정 값을 포함하는 컬럼만 추출
filterRule은 [컬럼번호, 찾으려는 값(문자열로 간주)]형태로 기입하며, 하나 이상의 규칙을 사용할 경우 논리 연산자인 "|" 혹은 "&"을 사용한다.
hadoop jar ankus-core2-etl-1.1.0.jar ETL  -input /data/etl.csv -output  /result/etl_FilterInclude -delimiter , -etlMethod FilterInclude -filterRule '0,5.4|1,3.1'
*입력 예제
........
5.1,2.5,3,1.1,"Versicolor"
5.7,2.8,4.1,1.3,"Versicolor"
6.3,3.3,6,2.5,"Virginica"
5.8,2.7,5.1,1.9,"Virginica"
7.1,3,5.9,2.1,"Virginica"
6.3,2.9,5.6,1.8,"Virginica"
6.5,3,5.8,2.2,"Virginica"
7.6,3,6.6,2.1,"Virginica"
4.9,2.5,4.5,1.7,"Virginica"
7.3,2.9,6.3,1.8,"Virginica"
6.7,2.5,5.8,1.8,"Virginica"
7.2,3.6,6.1,2.5,"Virginica"
6.5,3.2,5.1,2,"Virginica"
6.4,2.7,5.3,1.9,"Virginica"
6.8,3,5.5,2.1,"Virginica"
5.7,2.5,5,2,"Virginica"
5.8,2.8,5.1,2.4,"Virginica"
6.4,3.2,5.3,2.3,"Virginica"
.........
*출력 예제
**/result/etl_FilterExclude/part-r-00000
....
6.9,3.1,5.1,2.3,"Virginica"
6.7,3.1,5.6,2.4,"Virginica"
6.9,3.1,5.4,2.1,"Virginica"
6.4,3.1,5.5,1.8,"Virginica"
6.7,3.1,4.7,1.5,"Versicolor"
5.4,3,4.5,1.5,"Versicolor"
6.7,3.1,4.4,1.4,"Versicolor"
6.9,3.1,4.9,1.5,"Versicolor"
4.9,3.1,1.5,.2,"Setosa"
5.4,3.4,1.5,.4,"Setosa"
4.8,3.1,1.6,.2,"Setosa"
5.4,3.4,1.7,.2,"Setosa"
5.4,3.9,1.3,.4,"Setosa"
5.4,3.7,1.5,.2,"Setosa"
4.9,3.1,1.5,.1,"Setosa"
5.4,3.9,1.7,.4,"Setosa"
4.6,3.1,1.5,.2,"Setosa"
....

수행 예제3 : 입력 파일에서 특정 컬럼에서 특정 값을 포함하지 않는  컬럼만 추출
filterRule은 [컬럼번호, 찾으려는 값(문자열로 간주)]형태로 기입하며, 하나 이상의 규칙을 사용할 경우 논리 연산자인 "|" 혹은 "&"을 사용한다.
hadoop jar ankus-core2-etl-1.1.0.jar ETL  -input /data/etl.csv -output /result/etl_FilterExclude -delimiter , -etlMethod FilterExclude -filterRule '0,5.4|1,3.1'
*입력 예제
........
5.1,2.5,3,1.1,"Versicolor"
5.7,2.8,4.1,1.3,"Versicolor"
6.3,3.3,6,2.5,"Virginica"
5.8,2.7,5.1,1.9,"Virginica"
7.1,3,5.9,2.1,"Virginica"
6.3,2.9,5.6,1.8,"Virginica"
6.5,3,5.8,2.2,"Virginica"
7.6,3,6.6,2.1,"Virginica"
4.9,2.5,4.5,1.7,"Virginica"
7.3,2.9,6.3,1.8,"Virginica"
6.7,2.5,5.8,1.8,"Virginica"
7.2,3.6,6.1,2.5,"Virginica"
6.5,3.2,5.1,2,"Virginica"
6.4,2.7,5.3,1.9,"Virginica"
6.8,3,5.5,2.1,"Virginica"
5.7,2.5,5,2,"Virginica"
5.8,2.8,5.1,2.4,"Virginica"
6.4,3.2,5.3,2.3,"Virginica"
.........
*출력 예제
**/result/etl/part-r-00000
....
6.3,2.3,4.4,1.3,"Versicolor"
6,3.4,4.5,1.6,"Versicolor"
6,2.7,5.1,1.6,"Versicolor"
5.8,2.7,3.9,1.2,"Versicolor"
5.5,2.4,3.7,1,"Versicolor"
5.5,2.4,3.8,1.1,"Versicolor"
6.3,2.5,4.9,1.5,"Versicolor"
6.1,2.8,4,1.3,"Versicolor"
5.9,3.2,4.8,1.8,"Versicolor"
5.6,2.5,3.9,1.1,"Versicolor"
6.2,2.2,4.5,1.5,"Versicolor"
5.8,2.7,4.1,1,"Versicolor"
5.6,3,4.5,1.5,"Versicolor"
5.6,2.9,3.6,1.3,"Versicolor"
6.1,2.9,4.7,1.4,"Versicolor"
6,2.2,4,1,"Versicolor"
5.9,3,4.2,1.5,"Versicolor"
5,2,3.5,1,"Versicolor"
5.2,2.7,3.9,1.4,"Versicolor"
6.6,2.9,4.6,1.3,"Versicolor"
4.9,2.4,3.3,1,"Versicolor"
6.3,3.3,4.7,1.6,"Versicolor"
5.7,2.8,4.5,1.3,"Versicolor"
....

수행 예제4 : 입력 파일에서 특정 컬럼에서 특정 값을 다른 값으로 교체함.(문자열 교체 방식)
filterRule은 [컬럼번호, 찾으려는 값(문자열로 간주)]형태로 기입하며, 하나 이상의 규칙을 사용할 경우 논리 연산자인 "|" 혹은 "&"을 사용한다.
hadoop jar ankus-core2-etl-1.1.0.jar ETL  -input /data/etl.csv -output /result/etl_Replace -delimiter , -etlMethod Replace -ReplaceRule '0,5.1,x'
*입력 예제
........
5.1,2.5,3,1.1,"Versicolor"
5.7,2.8,4.1,1.3,"Versicolor"
6.3,3.3,6,2.5,"Virginica"
5.8,2.7,5.1,1.9,"Virginica"
7.1,3,5.9,2.1,"Virginica"
6.3,2.9,5.6,1.8,"Virginica"
6.5,3,5.8,2.2,"Virginica"
7.6,3,6.6,2.1,"Virginica"
4.9,2.5,4.5,1.7,"Virginica"
7.3,2.9,6.3,1.8,"Virginica"
6.7,2.5,5.8,1.8,"Virginica"
7.2,3.6,6.1,2.5,"Virginica"
6.5,3.2,5.1,2,"Virginica"
6.4,2.7,5.3,1.9,"Virginica"
6.8,3,5.5,2.1,"Virginica"
5.7,2.5,5,2,"Virginica"
5.8,2.8,5.1,2.4,"Virginica"
6.4,3.2,5.3,2.3,"Virginica"
.........
*출력 예제
**/result/etl_Replace/part-r-00000
....
4.8,3.1,1.6,.2,"Setosa"
4.7,3.2,1.6,.2,"Setosa"
5.2,3.4,1.4,.2,"Setosa"
5.2,3.5,1.5,.2,"Setosa"
5,3.4,1.6,.4,"Setosa"
5,3,1.6,.2,"Setosa"
4.8,3.4,1.9,.2,"Setosa"
x,3.3,1.7,.5,"Setosa"
4.6,3.6,1,.2,"Setosa"
x,3.7,1.5,.4,"Setosa"
5.4,3.4,1.7,.2,"Setosa"
x,3.8,1.5,.3,"Setosa"
5.7,3.8,1.7,.3,"Setosa"
x,3.5,1.4,.3,"Setosa"
5.4,3.9,1.3,.4,"Setosa"
5.7,4.4,1.5,.4,"Setosa"
5.8,4,1.2,.2,"Setosa"
4.3,3,1.1,.1,"Setosa"
....

수행 예제5 : 입력 파일에서 특정 컬럼에서 특정 조건(수치 범위)를 만족하는 값을 다른 값으로 교체 함.
교체 규칙은 [컬럼번호, 찾으려는 조건]형태로 기입한다.
hadoop jar ankus-core2-etl-1.1.0.jar ETL  -input /data/etl.csv -output /result/etl_NumericNorm -delimiter , -etlMethod NumericNorm -NumericForm '4<x<=5->Mid'
*입력 예제
........
5.1,2.5,3,1.1,"Versicolor"
5.7,2.8,4.1,1.3,"Versicolor"
6.3,3.3,6,2.5,"Virginica"
5.8,2.7,5.1,1.9,"Virginica"
7.1,3,5.9,2.1,"Virginica"
6.3,2.9,5.6,1.8,"Virginica"
6.5,3,5.8,2.2,"Virginica"
7.6,3,6.6,2.1,"Virginica"
4.9,2.5,4.5,1.7,"Virginica"
7.3,2.9,6.3,1.8,"Virginica"
6.7,2.5,5.8,1.8,"Virginica"
7.2,3.6,6.1,2.5,"Virginica"
6.5,3.2,5.1,2,"Virginica"
6.4,2.7,5.3,1.9,"Virginica"
6.8,3,5.5,2.1,"Virginica"
5.7,2.5,5,2,"Virginica"
5.8,2.8,5.1,2.4,"Virginica"
6.4,3.2,5.3,2.3,"Virginica"
.........
*출력 예제
**/result/etl_NumericNorm/part-r-00000
....
Mid,3.2,1.6,.2,"Setosa"
5.2,3.4,1.4,.2,"Setosa"
5.2,3.5,1.5,.2,"Setosa"
Mid,3.4,1.6,.4,"Setosa"
Mid,3,1.6,.2,"Setosa"
Mid,3.4,1.9,.2,"Setosa"
5.1,3.3,1.7,.5,"Setosa"
Mid,3.6,1,.2,"Setosa"
5.1,3.7,1.5,.4,"Setosa"
5.4,3.4,1.7,.2,"Setosa"
5.1,3.8,1.5,.3,"Setosa"
5.7,3.8,1.7,.3,"Setosa"
5.1,3.5,1.4,.3,"Setosa"
5.4,3.9,1.3,.4,"Setosa"
5.7,Mid,1.5,.4,"Setosa"
5.8,4,1.2,.2,"Setosa"
Mid,3,1.1,.1,"Setosa"
Mid,3,1.4,.1,"Setosa"
Mid,3.4,1.6,.2,"Setosa"
5.4,3.7,1.5,.2,"Setosa"
Mid,3.1,1.5,.1,"Setosa"
Mid,2.9,1.4,.2,"Setosa"
Mid,3.4,1.5,.2,"Setosa"
....

수행 예제6 : 입력 파일에서 특정 컬럼을 중심으로 오름 차순 정렬을 수행한다.
hadoop jar ankus-core2-etl-1.1.0.jar ETL  -input /data/etl.csv -output /result/etl_Sort -delimiter , -etlMethod Sort -Sort asc -SortTarget 1
*입력 예제
........
5.1,2.5,3,1.1,"Versicolor"
5.7,2.8,4.1,1.3,"Versicolor"
6.3,3.3,6,2.5,"Virginica"
5.8,2.7,5.1,1.9,"Virginica"
7.1,3,5.9,2.1,"Virginica"
6.3,2.9,5.6,1.8,"Virginica"
6.5,3,5.8,2.2,"Virginica"
7.6,3,6.6,2.1,"Virginica"
4.9,2.5,4.5,1.7,"Virginica"
7.3,2.9,6.3,1.8,"Virginica"
6.7,2.5,5.8,1.8,"Virginica"
7.2,3.6,6.1,2.5,"Virginica"
6.5,3.2,5.1,2,"Virginica"
6.4,2.7,5.3,1.9,"Virginica"
6.8,3,5.5,2.1,"Virginica"
5.7,2.5,5,2,"Virginica"
5.8,2.8,5.1,2.4,"Virginica"
6.4,3.2,5.3,2.3,"Virginica"
.........
*출력 예제
**/result/etl_NumericNorm/part-r-00000
5	2	3.5	1	Versicolor
6	2.2	5	1.5	Virginica
6	2.2	4	1	Versicolor
6.2	2.2	4.5	1.5	Versicolor
5	2.3	3.3	1	Versicolor
4.5	2.3	1.3	0.3	Setosa
5.5	2.3	4	1.3	Versicolor
6.3	2.3	4.4	1.3	Versicolor
4.9	2.4	3.3	1	Versicolor
5.5	2.4	3.7	1	Versicolor
5.5	2.4	3.8	1.1	Versicolor
5.1	2.5	3	1.1	Versicolor
6.7	2.5	5.8	1.8	Virginica
5.5	2.5	4	1.3	Versicolor
5.6	2.5	3.9	1.1	Versicolor
5.7	2.5	5	2	Virginica
....
4.3	3	1.1	0.1	Setosa
4.8	3	1.4	0.1	Setosa
4.9	3	1.4	0.2	Setosa
6.7	3.1	5.6	2.4	Virginica
6.9	3.1	5.1	2.3	Virginica
4.9	3.1	1.5	0.2	Setosa
6.9	3.1	4.9	1.5	Versicolor
4.6	3.1	1.5	0.2	Setosa
6.7	3.1	4.4	1.4	Versicolor
4.8	3.1	1.6	0.2	Setosa
6.7	3.1	4.7	1.5	Versicolor
...
5.4	3.7	1.5	0.2	Setosa
7.7	3.8	6.7	2.2	Virginica
7.9	3.8	6.4	2	Virginica
5.1	3.8	1.6	0.2	Setosa
5.1	3.8	1.9	0.4	Setosa
5.1	3.8	1.5	0.3	Setosa
5.7	3.8	1.7	0.3	Setosa
5.4	3.9	1.3	0.4	Setosa
5.4	3.9	1.7	0.4	Setosa
5.8	4	1.2	0.2	Setosa
5.2	4.1	1.5	0.1	Setosa
5.5	4.2	1.4	0.2	Setosa
5.7	4.4	1.5	0.4	Setosa
....


수치 데이터에 대해 확률 가중치를 기반으로 소속 확률이 높은 것끼리 묶는 군집 알고리즘입니다.

수행방법:
hadoop jar ankus-core2-fuzzy-k-means-1.1.0.jar FuzzyKMeans \
-input [입력 파일 또는 입력 폴더] \
-output [출력 데이터 폴더 경로] \
-indexList [읽을 데이터의 컬럼 인덱스 리스트] \
-delimer [컬럼란 구분자] \
-k [클러스터 수<0 이상의 양의 정수>] \
-p [클러스터 가중치<0에서1사이의 실수>] \
-maxIteration [군집 할당 반복수<1이상의 양의 실수>]

수행 예제:
hadoop jar ankus-core2-fuzzy-k-means-1.1.0.jar \
FuzzyKMeans \
-input /data/kmeans.csv \
-output /result/fuzzy-kmeans_csv -delimiter , \
-indexList 0,1,2,3 \
-k 3 \
-p 0.1 \
-maxIteration 10

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
**/result/fuzzy-kmeans_csv/finalResult/part-m-00000 클러스터 할당 결과
[변수1, 변수2, 변수3, 변수4, 군집번호]
.....
5,3.2,1.2,0.2,0
5.5,3.5,1.3,0.2,0
4.9,3.1,1.5,0.1,0
4.4,3,1.3,0.2,0
5.1,3.4,1.5,0.2,0
5,3.5,1.3,0.3,0
4.5,2.3,1.3,0.3,0
4.4,3.2,1.3,0.2,0
5,3.5,1.6,0.6,0
5.1,3.8,1.9,0.4,0
4.8,3,1.4,0.3,0
5.1,3.8,1.6,0.2,0
4.6,3.2,1.4,0.2,0
5.3,3.7,1.5,0.2,0
5,3.3,1.4,0.2,0
7,3.2,4.7,1.4,2
6.4,3.2,4.5,1.5,1
6.9,3.1,4.9,1.5,2
5.5,2.3,4,1.3,1
6.5,2.8,4.6,1.5,1
5.7,2.8,4.5,1.3,1
6.3,3.3,4.7,1.6,1
4.9,2.4,3.3,1,1
6.6,2.9,4.6,1.3,1
5.2,2.7,3.9,1.4,1
5,2,3.5,1,1
....
**/result/fuzzy-kmeans_csv/Centroid/part-r-00000 : 각 속성별 클러스터 소속 비율
[군집번호, 변수번호, 가중치]
0	0,5.0035
0	1,3.4037
0	2,1.4839
0	3,0.2511
1	0,5.8755
1	1,2.7564
1	2,4.3441
1	3,1.387
2	0,6.7585
2	1,3.0474
2	2,5.626
2	3,2.0454


수치/범주형 데이터에 대해 유클리드/멘허튼 거리 측정 방법을 이용하여 가까운 것끼리 묶어주는 군집 알고리즘입니다.

수행 방법:
hadoop jar ankus-core2-kmeans-1.1.0.jar \
KMeans \
-input [입력 파일 또는 입력 폴더] \
-output [출력 데이터 폴더 경로] \
-delimiter [속성 구분자]  \
-indexList [수치형 속성 목록] \
-clusterCnt [클러스터 개수]  \
-convergeRate [클러스터 중심의 이동 변화 수렴 값] \
-maxIteration [군집 반복 횟수] \
-finalResultGen [클러스터내의 데이터 갯수 출력 여부]

-clusterCnt : 정수 값으로 1이상 데이터 수 이하로 설정
-convergeRate : 0에서 1사이의 실수 값.
-maxIteration : 0에상 정수 값 
-finalResultGen true로 설정시

purity.csv : 군집의 데이터 분포
clustering_result.csv : 군집 결과
clustering_result/part-m-00000 : clustering_result.csv과 동일(엑셀 호환용으로 생성됨)

-finalResultGen false로 설정시
clustering_result_[군집 반복 횟수] /part-m-00000

수행 예제:
hadoop jar ankus-core2-kmeans-1.1.0.jar \
KMeans \
-input /data/kmeans.csv \
-output /result/kmeans_csv \
-delimiter , \
-indexList 0,1,2,3 \
-clusterCnt 4 \
-convergeRate 0 \
-maxIteration 100 \
-finalResultGen true

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
**purity.csv : 군집의 데이터 분포
" Clustering Result - Purity
" Cluster Number,Assigned Data Count,Assigned Data Ratio
" Attr-4,frequency,ratio
0,27,0.18
1,23,0.153
2,61,0.407
3,39,0.26
**clustering_result.csv : 군집의 데이터 분포
...
6.9,3.2,5.7,2.3,3,0.279
5.6,2.8,4.9,2,2,0.816
7.7,2.8,6.7,2,3,1.329
6.3,2.7,4.9,1.8,2,0.755
6.7,3.3,5.7,2.1,3,0.275
7.2,3.2,6,1.8,3,0.53
6.2,2.8,4.8,1.8,2,0.638
6.1,3,4.9,1.8,2,0.714
6.4,2.8,5.6,2.1,3,0.546
7.2,3,5.8,1.6,3,0.582
7.4,2.8,6.1,1.9,3,0.739
7.9,3.8,6.4,2,3,1.445
6.4,2.8,5.6,2.2,3,0.563
....


거리 기반 데이터 분류 모델

수행 방법:
hadoop jar ankus-core2-kNN-1.1.0.jar \
kNN \
-input [입력 파일 또는 입력 폴더] \
-output [출력 폴더 경로] \
-delimiter [컬럼 구분자] \
-modelPath [모델 데이터 경로] \
-indexList [분류에 사용할 수치형 데이터 컬럼 인덱스 리스트] \
-nominalIndexList [분류에 사용할 범주형 데이터 컬럼 인덱스 리스트] \ 
-classIndex [분류에 사용할 레이블 컬럼 인덱스 리스트] \
-k [인접한 인스턴스의 갯수] \
-distanceOption [인스턴스간 거리 측정 방법{manhattan|uclidean}] \ 
-distanceWeight [인스턴스간 거리의 가중치{true|false}]  \
-isValidation [모델 검증 결과 출력 여부{true|false}]

-모델 데이터 경로 생략 시 입력 데이터가 모델로 사용됨. 
-입력 데이터의 속성이 수치형으로만 존재할 경우 nominalIndexList를 생략함.
-입력 데이터의 속성이 범주형으로만 존재할 경우 indexList를 생략함.
-모델 데이터 사용 여부: true로 설정시 
-모델 데이터 경로 사용함.
-인접한 인스턴스의 갯수 : 양의 정수
-인스턴스간 거리 측정 방법: manhattan, uclidean 지원
-인스턴스간 거리의 가중치:false의 경우 k개 클래스의 단순 투표로 결정.
-모델 검증 결과 출력 여부: true 로 설정 시 /result/kNN/validation가 생성됨.
-분류 결과 출력 경로 : 출력 폴도 경로: /classifying_result/part-r-00000가 자동 생성.

수행 예제:
hadoop jar ankus-core2-kNN-1.1.0.jar kNN -input /data/kNN_input.csv -output /result/kNN -delimiter , -modelPath /data/kNN_model.csv -indexList 0,1,2,3 -classIndex 4 -k 3 -distanceOption uclidean -distanceWeight false  -isValidation true
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
**/result/kNN/validation 
" Total Summary
Total Instances: 10
Correctly Classified Instances: 10(100.00%)
Incorrectly Classified Instances: 0(0.00%)

" Confusion Matrix
(Classified as)	setosa	|	total	
setosa	10	|	"10
total	10

" Detailed Accuracy
Class	TP_Rate	FP_Rate	Precision	Recall	F-Measure
setosa	1.000	0.000	1.000	1.000	1.000
Weig.Avg.	1.000	0.000	1.000	1.000	1.000

**/result/kNN/classifying_result/part-r-00000
5.1,3.5,1.4,0.2,setosa,setosa
4.9,3,1.4,0.2,setosa,setosa
4.7,3.2,1.3,0.2,setosa,setosa
4.6,3.1,1.5,0.2,setosa,setosa
5,3.6,1.4,0.2,setosa,setosa
5.4,3.9,1.7,0.4,setosa,setosa
4.6,3.4,1.4,0.3,setosa,setosa
5,3.4,1.5,0.2,setosa,setosa
4.4,2.9,1.4,0.2,setosa,setosa
4.9,3.1,1.5,0.1,setosa,setosa

확률 기반 데이터 분류 모델

수행 방법:
hadoop jar ankus-core2-naivebayes-1.1.0.jar \
NaiveBayes \
-input [입력 파일 또는 입력 폴더] \
-output [출력 폴더 경로] \
-indexList [수치형 대상 인덱스 리스트] \
-nominalIndexList [범주형 대상 인덱스 리스트] \
-classIndex [클래스 레이블 인덱스] \
-delimiter [컬럼 구분자] \
-finalResultGen true

입력 데이터의 속성이 수치형으로만 존재할 경우 nominalIndexList를 생략함.
입력 데이터의 속성이 범주형으로만 존재할 경우 indexList를 생략함.
-finalResultGen false로 설정, 혹은 사용하지 않을 경우  모델만 생성

-finalResultGen true로 설정시 
bayes_rules, classifying_result/part-m-00000,  validation가 생성됨.
bayes_rules  : 모델 파일
classifying_result/part-m-00000 : 분류 결과 파일
validation : 성능 검증 결과 파일

수행 예제:

-모델 생성:
hadoop jar ankus-core2-naivebayes-1.1.0.jar \
NaiveBayes  \
-input /data/naivebayes.csv \
-output /result/naive_bayes_model \
-indexList 0,1,2,3 \
-classIndex 4 \
-finalResultGen true \
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
**/result/naive_bayes_model/bayes_rules  : 모델 파일
" AttrIndex(or Class),Type,Value(Category or Avg/StdDev),ValueCount,ClassType,ClassCount
0,numeric,5.006,0.3489469873777273,50,setosa,50
1,numeric,3.4179999999999997,0.3771949098278028,50,setosa,50
2,numeric,1.464,0.1717672844286691,50,setosa,50
3,numeric,0.244,0.10613199329137288,50,setosa,50
0,numeric,5.936,0.5109833656783921,50,versicolor,50
1,numeric,2.7700000000000005,0.3106444913401753,50,versicolor,50
2,numeric,4.26,0.46518813398452424,50,versicolor,50
3,numeric,1.3259999999999996,0.19576516544063882,50,versicolor,50
0,numeric,6.587999999999998,0.6294886813915159,50,virginica,50
1,numeric,2.9739999999999993,0.31925538366644257,50,virginica,50
2,numeric,5.551999999999998,0.5463478745268635,50,virginica,50
3,numeric,2.025999999999999,0.271889683511538,50,virginica,50
class,virginica,50,150
class,setosa,50,150
class,versicolor,50,150
**/result/naive_bayes_model/classifying_result/part-m-00000 : 분류 결과 파일
..............
5.7,2.6,3.5,1,versicolor,versicolor
5.5,2.4,3.8,1.1,versicolor,versicolor
5.5,2.4,3.7,1,versicolor,versicolor
5.8,2.7,3.9,1.2,versicolor,versicolor
6,2.7,5.1,1.6,versicolor,versicolor
5.4,3,4.5,1.5,versicolor,versicolor
6,3.4,4.5,1.6,versicolor,versicolor
6.7,3.1,4.7,1.5,versicolor,versicolor
6.3,2.3,4.4,1.3,versicolor,versicolor
................
**/result/naive_bayes_model/validation : 성능 검증 결과 파일
" Total Summary
Total Instances: 150
Correctly Classified Instances: 144(96.00%)
Incorrectly Classified Instances: 6(4.00%)

" Confusion Matrix
(Classified as)	setosa	versicolor	virginica	|	total	
setosa	50	0	0	|	"50
versicolor	0	47	3	|	"50
virginica	0	3	47	|	"50
total	50	50	50

" Detailed Accuracy
Class	TP_Rate	FP_Rate	Precision	Recall	F-Measure
setosa	1.000	0.000	1.000	1.000	1.000
versicolor	0.940	0.030	0.940	0.940	0.940
virginica	0.940	0.030	0.940	0.940	0.940
Weig.Avg.	0.960	0.020	0.960	0.960	0.960

-테스트: 
hadoop jar ankus-core2-naivebayes-1.1.0.jar \
NaiveBayes  \
-input /data/naivebayes.csv \
-modelPath /result/naive_bayes_model/bayes_rules \
-output /result/naive_bayes_test \
-indexList 0,1,2,3 \
-classIndex 4 \
-delimiter ,

**/result/naive_bayes_test/validation : 성능 검증 결과 파일
" Total Summary
Total Instances: 150
Correctly Classified Instances: 144(96.00%)
Incorrectly Classified Instances: 6(4.00%)

" Confusion Matrix
(Classified as)	setosa	versicolor	virginica	|	total	
setosa	50	0	0	|	"50
versicolor	0	47	3	|	"50
virginica	0	3	47	|	"50
total	50	50	50

" Detailed Accuracy
Class	TP_Rate	FP_Rate	Precision	Recall	F-Measure
setosa	1.000	0.000	1.000	1.000	1.000
versicolor	0.940	0.030	0.940	0.940	0.940
virginica	0.940	0.030	0.940	0.940	0.940
Weig.Avg.	0.960	0.020	0.960	0.960	0.960

**/result/naive_bayes_test/classifying_result/part-m-00000 : 분류 결과 파일
5,3.5,1.3,0.3,setosa,setosa
4.5,2.3,1.3,0.3,setosa,setosa
4.4,3.2,1.3,0.2,setosa,setosa
5,3.5,1.6,0.6,setosa,setosa
5.1,3.8,1.9,0.4,setosa,setosa
4.8,3,1.4,0.3,setosa,setosa
5.1,3.8,1.6,0.2,setosa,setosa
4.6,3.2,1.4,0.2,setosa,setosa
5.3,3.7,1.5,0.2,setosa,setosa
5,3.3,1.4,0.2,setosa,setosa
7,3.2,4.7,1.4,versicolor,versicolor
6.4,3.2,4.5,1.5,versicolor,versicolor
6.9,3.1,4.9,1.5,versicolor,virginica
5.5,2.3,4,1.3,versicolor,versicolor
6.5,2.8,4.6,1.5,versicolor,versicolor
5.7,2.8,4.5,1.3,versicolor,versicolor
6.3,3.3,4.7,1.6,versicolor,versicolor
4.9,2.4,3.3,1,versicolor,versicolor
6.6,2.9,4.6,1.3,versicolor,versicolor
5.2,2.7,3.9,1.4,versicolor,versicolor

트리 기반 연관 분석 알고리즘


수행방법:
hadoop jar ankus-core2-FPGrowth-1.1.0.jar PFP-growth \
-input [입력 파일 또는 입력 폴더] \
-output [출력 데이터 폴더 경로] \
-targetItemList [찾으려는 아이템 목록으로 ,로 구분됨] \
-delimiter [아이템 간 구분자] \
-maxRuleLength [최대 규칙 길이로 1이상의 정수] \
-ruleCount [생성된 규칙에서 metricValue에 따라 추출할  순위 N개] \
-metricType [규칙평가 척도{confidence|lift}] \
-metricValue [척도 값으로 0이상의 실수 값.] \
-minSup [0에서 1사이의 실수(0,1은 제외)]

수행 예제:lift 사용
hadoop jar ankus-core2-FPGrowth-1.1.0.jar \
PFP-growth \
-minSup 0.2 \
-metricType lift \
-metricValue 0 \
-input /data/pfpgrowth.txt \
-output /result/PFPGrowth \
-delimiter \t \
-maxRuleLength 3 \
-ruleCount 5

*입력 예제
1	2	5
2	4
2	3
1	2	4
1	3
2	3
1	3
1	2	3	5
1	2	3
*출력 예제
/result/PFPGrowth/rule_result.txt
1@@3	5@@1@@lift@@0.09
2@@3	5@@2@@lift@@0.09
3@@1	4@@2@@lift@@0.09
4@@2	5@@1@@lift@@0.09
5@@1	5@@2@@lift@@0.09

수행 예제:confidence 사용
hadoop jar ankus-core2-FPGrowth-1.1.0.jar PFP-growth -minSup 0.2 -metricType lift -metricValue 0 -input /data/pfpgrowth.txt -output /result/PFPGrowth -delimiter \t -maxRuleLength 3 -ruleCount 5
*입력 예제
1	2	5
2	4
2	3
1	2	4
1	3
2	3
1	3
1	2	3	5
1	2	3
*출력 예제
/result/PFPGrowth/rule_result.txt
1@@4@@2@@confidence@@1.0
2@@3	5@@1@@confidence@@1.0
3@@3	5@@2@@confidence@@1.0
4@@1	4@@2@@confidence@@1.0
5@@5@@1	2@@confidence@@1.0

문서내 단어의 중요도를 분석하는 모듈

입력 데이터는 하나의 문서가 1라인으로 형성되어야 함.
1개 라인은 문서제목 구분자 본문으로 구성됨.

수행 방법:
hadoop jar ankus-core2-TFIDF-1.1.0.jar \
TFIDF \
-input [입력 데이터 파일 경로] \
-output [출력 데이터 폴더 경로] \
-delimiter [문서 제목과 본문의 구분자]

수행 예제:
hadoop jar ankus-core2-TFIDF-1.1.0.jar \
TFIDF\
-input /data/tf-idf.txt \
-output /result/tf-idf

*입력 파일:
**입력 데이터는 하나의 문서가 1라인으로 형성되어야 함.
**1개 라인은 문서제목 구분자 본문으로 구성됨.
Entertainment	South Korea's major theaters are at loggerheads with the global streaming giant Netflix over the release of its latest production "Okja," which they fear will upend the local movie distribution industry.Netflix has announced a plan to make the latest film by internationally acclaimed South Korean director Bong Joon-ho available simultaneously on the online platform and South Korean theaters. But South Korea's three largest multi-screen chains -- CJ CGV, Lotte Cinema and Megabox,  which control 90 percent of screens in the country -- finally decided not to release the movie. They argued that the simultaneous release would "destroy the ecosystem of the local film industry."In South Korea, movies are offered on Internet-based TV, VOD and other platforms about two to three weeks after their big-screen debuts.As both sides refused to step back, the international action film is expected to be shown only in non-multiplex theaters June 29. As of Monday, a total of 79 theaters with a total of 103 screens around the country decided to release the movie, and the number will continue to rise till the release day, according to the movie's local distributor Next Entertainment World.Co-written by Bong and Jon Ronson of "Frank," "Okja" follows a girl from a rural town who risks everything to prevent a multinational company from kidnapping her close friend and super pig named Okja. The sci-fi film was co-produced by three Hollywood studios -- Plan B, Lewis Pictures and Kate Street Picture Co. -- while Netflix covered the film's entire budget of US$50 million.It stars Tilda Swinton, Jake Gyllenhaal of "Nightcrawler" and "Everest," and Paul Dano of "Love & Mercy" and "12 Years a Slave," and has Korean actors, including An Seo-hyun, Byun Hee-bong, Choi Woo-shik and Yoon Je-moon, among its cast.Despite the major theaters' boycott, "Okja" remained second on the real-time chart of pre-reserved movies after the forthcoming Hollywood blockbuster "Transformer: The Last Knight" for the second day in a row with nine days left before its release.But it was even before the movie comes to South Korea that it stirred heated debate over Netflix movie's theatrical release. The debate first surfaced during the 70th Cannes Film Festival last month where "Okja" was one of the two Netflix titles competing in Cannes for the first time, along with Noah Baumbach's "The Meyerowitz Stories." But after a strong protest from French movie theaters, Cannes changed its rules to exclude films without a commitment for a French theatrical release starting next year.Behind the boycott seems to be mounting fear that the global streaming giant would undermine the country's film distribution system and eventually dominate the film market. Netflix has only about 80,000 paid subscribers in Korea but over 100 million worldwide. The number of international subscribers could reach 128 million by 2022, according to a report from the US Digital TV Research."The reason why we refuse to release this profitable much-anticipated film is because we don't know what influence it will have on the Korean cinema market in the long term," said an official at one of the cinema chains participating in the boycott, asking not to be named. "When it comes to the power of Netflix, domestic movie chains are rather weak," a CGV official said, also requesting not to be named. "Since Netflix is expanding its influence not only in movies but also in broadcast content, we need to first have in-depth discussions on what influence it will have on movie, online video and the overall broadcasting market in the future."However, not all Korean moviegoers are in favor of domestic cinema chains as far as this contentious issue is concerned. Some claim that the movie chains are not qualified to mention "an ecosystem of the film industry," while they are busily filling theaters with lucrative films and increasing the number of chain theaters."It's hard to swallow the multiplex operators' claim that they cannot simultaneously release the movie due to conventions even though they have already introduced policies running against market traditions like charging different prices for seats," Kim Hyong-ho, a movie market analyst, pointed out.Conglomerates have come under fire for monopolizing the market by operating investment, production, distribution and screening all together, and thus excluding independent and experimental films from their theaters. Last year, they came under criticism for beginning to charge new rates, which actually hiked ticket prices for the majority of moviegoers who watch movies during the prime hours on weekends.Some film critics raised the need for the domestic film industry to adapt to the fast-changing media environment."You cannot swim against the trend of the times where the ecosystem of films and their consumers have changed," said Jang Ji-wook, a movie critic. "I think the local movie industry are responding slowly to the changes." Experts say whether "Okja" gets a theatrical release or not Netflix has little to lose either way.Bong, one of the most trusted and favored movie directors in South Korea, could prompt viewers to subscribe to the online streaming service to watch his new film, even if the movie is not shown in theaters. If shown in theaters, the streaming service can get extra revenue.Most of all, the noise created by the recent debate could help increase the public recognition of the US brand in the South Korean streaming market, now dominated by big mobile carriers. Bong said during a news conference to promote the film on June 14 that it was him who attached the condition of the film being shown both on the big screen and on the online platform in Korea."The controversy was caused due to my cinematic selfishness. I am the one who provided the cause," Bong said. "I fully understand the multiplexes asking for a minimum three-week hold back.Netflix's principle for a simultaneous release should also be respected."He also expressed hope that "Okja" would become a "signal flare" in establishing new rules relating to similarly produced films
Sports	South Korea will seek North Korea's participation in a continental weightlifting competition to be held this fall south of the border, Seoul's sports minister said Wednesday.Do Jong-hwan, the minister of culture, sports and tourism, said South Korea will try to bring North Korean athletes on board for the 2017 Asian Cup and Interclub Weightlifting Championship starting on Oct. 18 in Yanggu, some 175 kilometers east of Seoul.In this file photo taken on Sept. 25, 2014, North Korean weightlifter Kim Un-ju holds up her national flag after winning the gold medal in the women`s 75kg class at the Incheon Asian Games in Incheon. (Yonhap)An official with the Korea Weightlifting Federation said securing North Korea's participation "is one of our most important goals" for the event."We have asked the Asian Weightlifting Federation to help us in that regard," the official said. "Some of our officials may run into North Korean officials at international competitions in a third country. We can discuss the Asian Cup participation then." North Korea is one of Asia's premier weightlifting powers. Its athletes last competed south of the border during the 2014 Incheon Asian Games. No North Korean weightlifter has participated in a single weightlifting event in the South. South Korean lifters competed at the 2013 Asian Cup held in Pyongyang. North Korea also permitted the hoisting of the South Korean flag and playing of its anthem in the North Korean capital for the first time in a sporting event Despite lingering tensions on the divided peninsula, the two Koreas have engaged in a series of sporting exchanges this year. In April, the South Korean women's football team faced North Korea in Pyongyang, while the North Korean women's hockey team took on South Korea in Gangneung, some 230 kilometers east of Seoul. On Saturday, a North Korean taekwondo demonstration team will have a joint performance with South Koreans in the opening ceremony of the World Taekwondo Federation World Taekwondo Championships in Muju, 240 kilometers south of the capital. (Yonhap)
LifeStyle	Summer package at Sheraton Seoul D Cube CitySheraton Seoul D Cube City Hotel is offering a summer package that includes a one-night stay in a Club Room, express check-in at the Club Lounge on the 38th floor overlooking the city, continental breakfast for two and happy hour coffee, tea and snacks.Guests also receive an eco bag with a teddy bear, skincare products and other goodies. The package starts from 290,000 won and runs until Aug. 31. For information and reservation, call (02) 2211-2100.Japanese delicacies at Park Hyatt Seoul Park Hyatt Seoul’s Japanese restaurant and bar, The Timber House, is offering a golden hour promotion featuring a Japanese light course meal of a beef sandwich, uni soba, green tea pudding, sake and Japanese beer.The meal is available daily from 6 p.m. to 8 p.m. until Aug. 31 and priced at 59,000 won per person. The Timber House is designed like a traditional Korean house and provides a cozy atmosphere. Live music is played from 6:30 p.m. to 11 p.m. from Monday through Thursday, and from 7:30 pm to 12 am on Friday and Saturday. For information and reservation, call (02) 2016-1290 or (02) 2016-1234 Summer packages at Novotel Ambassador Seoul Novotel Ambassador Seoul Gangnam is offering summer packages until Aug. 31 for families, friends and couples. The Summer City Getaway package features a one-night stay in a standard room, access to fitness facilities and a swimming pool as well as a 20 percent discount at the hotel’s restaurants. It starts from 165,000 won and above. The choice of a superior room includes a breakfast buffet for two, shaved ice bingsoo and two cups of juice as well as access to a sauna.The Summer Weekend Escape package includes a one-night stay in a standard room and a breakfast buffet for two. Guests can check out at 8 p.m. on Sunday. The package starts from 249,000 won.All packages offer access to a fitness center and a swimming pool as well as a 20 percent discount at hotel restaurants. For information and reservation, call (02) 531-6520-1. Lobster special at Millennium Seoul Hilton Millennium Seoul Hilton’s Italian restaurant Il Ponte is offering a lobster promotion until the end of June. The six-course meal starts with Wagyu beef carpaccio served with arugula salsa verde and parmesan crisps, followed by a creamy asparagus soup with herb gnocchi. Next comes a lobster and spinach quiche with bacon bites. The main dish is a choice of a sauteed live lobster and Wagyu tenderloin served with herbs, zucchini, mushrooms, white wine and cream roasted baby potatoes, or a marinated live lobster served with vegetables and mango puree.A menu for two people is also available with a choice of four kinds of cooked lobsters. For dessert, a pear marinated in red wine is served with vanilla bourbon ice cream, followed by coffee or tea. For information and reservations, call (02) 317-3270. Lobster promotion at Conrad Seoul The 37 Grill & Bar located on the 37th floor of Conrad Seoul is offering a promotion of lobster dishes until June 30.The six-course menu features an amuse-bouche as appetizer, followed by a cold lobster melon soup and bisque risotto with lobster meat. Pina Colada sorbet is offered before the main dish of grilled Wagyu strip loin steak with butter-poached lobster and ginseng-celeriac puree. Vanilla ice cream is offered as dessert. The price is 145,000 won per person. For information and reservation, call (02) 6137-7110.
Economy	South Korea's public sector net lending hit a record high last year, thanks to a robust gain in tax revenues and lower oil prices, data showed Wednesday. Net lending, which refers to total revenue minus total expenditures, came to 43.9 trillion won ($38.4 billion) in 2016, compared with 32.9 trillion won from a year earlier, according to preliminary data from the Bank of Korea.It was the highest annual net lending since the central bank began compiling statistics on the public sector account. Last year, total revenue in the public sector rose 4.1 percent on-year to 765.1 trillion won, while total expenditures climbed 2.7 percent on-year to 721.2 trillion won.Kim Seong-ja, an official at the central bank, attributed the rise in public sector net lending to a growth in tax revenues and spending cuts on lower oil prices.Tax revenues jumped 27.9 percent on-year to 319.1 trillion won last year. The central government posted a deficit of 14 trillion won last year, down from 29.5 trillion won in the red in 2015. The deficit of central and provincial governments in South Korea accounted for 0.5 percent of the nominal gross domestic product in 2016, compared with an average 3 percent among member countries of the Organization for Economic Cooperation and Development, according to the central bank data. (Yonhap)

*출력 파일:
**/result/tf-idf/Economy-r-00000
TRILLION,0.0337,0.6021,0.020
CENTRAL,0.024,0.6021,0.015
LENDING,0.0192,0.6021,0.012
BANK,0.0192,0.6021,0.012
NET,0.0192,0.6021,0.012
SECTOR,0.0192,0.6021,0.012
WON,0.0337,0.301,0.010
TAX,0.0144,0.6021,0.009
REVENUES,0.0144,0.6021,0.009
DATA,0.0144,0.6021,0.009
LOWER,0.0096,0.6021,0.006
PUBLIC,0.0192,0.301,0.006
DEFICIT,0.0096,0.6021,0.006
TOTAL,0.0192,0.301,0.006
OIL,0.0096,0.6021,0.006
COMPARED,0.0096,0.6021,0.006
EXPENDITURES,0.0096,0.6021,0.006
YEAR,0.0385,0.1249,0.005
PERCENT,0.024,0.1249,0.003
PRICES,0.0096,0.301,0.003
MINUS,0.0048,0.6021,0.003
COMPILING,0.0048,0.6021,0.003
ECONOMIC,0.0048,0.6021,0.003
.............

**/result/tf-idf/Entertainment-r-00000
MOVIE,0.0159,0.6021,0.010
FILM,0.0149,0.6021,0.009
RELEASE,0.0119,0.6021,0.007
THEATERS,0.0109,0.6021,0.007
NETFLIX,0.0099,0.6021,0.006
NOT,0.0079,0.6021,0.005
ARE,0.0069,0.6021,0.004
OKJA,0.0069,0.6021,0.004
MARKET,0.0069,0.6021,0.004
THEY,0.006,0.6021,0.004
BONG,0.006,0.6021,0.004
FILMS,0.005,0.6021,0.003
CHAINS,0.005,0.6021,0.003
INDUSTRY,0.005,0.6021,0.003
BUT,0.005,0.6021,0.003
STREAMING,0.005,0.6021,0.003
WHO,0.004,0.6021,0.002
ONLINE,0.004,0.6021,0.002
SHOWN,0.004,0.6021,0.002
MOVIES,0.004,0.6021,0.002
THREE,0.004,0.6021,0.002
CINEMA,0.004,0.6021,0.002
LOCAL,0.004,0.6021,0.002
BY,0.0069,0.301,0.002
WOULD,0.003,0.6021,0.002
.......
**/result/tf-idf/LifeStyle-r-00000
LOBSTER,0.0179,0.6021,0.011
PACKAGE,0.0107,0.6021,0.006
SUMMER,0.0107,0.6021,0.006
CALL,0.0089,0.6021,0.005
UNTIL,0.0089,0.6021,0.005
P,0.0089,0.6021,0.005
OFFERING,0.0089,0.6021,0.005
SEOUL,0.0179,0.301,0.005
M,0.0089,0.6021,0.005
INFORMATION,0.0089,0.6021,0.005
AS,0.0143,0.301,0.004
PROMOTION,0.0071,0.6021,0.004
JAPANESE,0.0071,0.6021,0.004
SERVED,0.0071,0.6021,0.004
ROOM,0.0071,0.6021,0.004
RESERVATION,0.0071,0.6021,0.004
STARTS,0.0071,0.6021,0.004
PACKAGES,0.0054,0.6021,0.003
STAY,0.0054,0.6021,0.003
AUG,0.0054,0.6021,0.003
CITY,0.0054,0.6021,0.003
FOLLOWED,0.0054,0.6021,0.003
LIVE,0.0054,0.6021,0.003
CREAM,0.0054,0.6021,0.003
ACCESS,0.0054,0.6021,0.003
INCLUDES,0.0054,0.6021,0.003
WAGYU,0.0054,0.6021,0.003
COURSE,0.0054,0.6021,0.003
....
**/result/tf-idf/Sports-r-00000
NORTH,0.0359,0.6021,0.022
WEIGHTLIFTING,0.018,0.6021,0.011
ASIAN,0.018,0.6021,0.011
EVENT,0.009,0.6021,0.005
TAEKWONDO,0.009,0.6021,0.005
INCHEON,0.009,0.6021,0.005
TEAM,0.009,0.6021,0.005
CUP,0.009,0.6021,0.005
PARTICIPATION,0.009,0.6021,0.005
WOMEN,0.009,0.6021,0.005
KILOMETERS,0.009,0.6021,0.005
FEDERATION,0.009,0.6021,0.005
SOUTH,0.0329,0.1249,0.004
KOREAN,0.0299,0.1249,0.004
HELD,0.006,0.6021,0.004
WEIGHTLIFTER,0.006,0.6021,0.004
SPORTING,0.006,0.6021,0.004
SAID,0.012,0.301,0.004
FLAG,0.006,0.6021,0.004
ATHLETES,0.006,0.6021,0.004
MINISTER,0.006,0.6021,0.004
PYONGYANG,0.006,0.6021,0.004
OUR,0.006,0.6021,0.004
SPORTS,0.006,0.6021,0.004
GAMES,0.006,0.6021,0.004
BORDER,0.006,0.6021,0.004
OFFICIALS,0.006,0.6021,0.004
EAST,0.006,0.6021,0.004
COMPETED,0.006,0.6021,0.004
CAPITAL,0.006,0.6021,0.004
KOREA,0.0269,0.1249,0.003
WILL,0.009,0.301,0.003
THIS,0.009,0.301,0.003
SEOUL,0.009,0.301,0.003
HAVE,0.009,0.301,0.003
..............
