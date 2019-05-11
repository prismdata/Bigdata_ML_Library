불린 값을 갖는 객체간 유사도를 추정하는 알고리즘.

수행 방법:
hadoop jar ankus-core2-boolean-sim-1.1.0.jar BooleanDataCorrelation \
-input [입력 파일 또는 입력 폴더]  \
-output [출력 데이터 폴더] \
-delimiter [속성간 구분자] \
 -keyIndex [데이터 식별자를 위한 기준 키(Unique key)가 되는 컬럼] \
 -algorithmOption [유사도/거리 계산 옵션 선택 {dice | jaccard | hamming}]

수행 예제:
hadoop jar ankus-core2-boolean-sim-1.1.0.jar BooleanDataCorrelation \
-input /data/boolean_sim.csv \
-output /result/boolean_similarity \
-indexList 0 \
-delimiter , \
-keyIndex 0 \
-algorithmOption dice

*입력 예제
**/data/boolean_sim.csv
u,1,0,1,1,1,1,0,0
w,1,1,1,0,1,1,1,0

*출력 예제
**/result/boolean_similarity/part-r-00000
u	w	0.909