추천 알고리즘의 사용자간 유사도 모델 생성 

수행 방법:
hadoop jar ankus-core2-userbased-Recommend-1.1.0.jar \
UserBased_Recommendation \
-input [입력 데이터 경로] \
-output [출력 파일이 생성될 경로] \
-delimiter [사용자, 아이템 컬럼 구분자] \
-uidIndex [사용자 컬럼 번호] \
-iidIndex [아이템 컬럼 번호] \
-ratingIndex [평점 컬럼 인덱스] \
-similPath [사용자간 유사도 데이터 경로] \
-basedType [유사도 타입{사용자, 아이템}]  \
-similDelimiter [유사도 데이터의 컬럼 구분자] \
-similThreshold [추천 생성시에 사용할 유사 사용자과의 유사도 임계값] \
-targetUID [추천을 생성하여 제공할 대상 사용자의 ID] \
-targetIIDList [대상 사용자에게 추천할 대상 아이템의 목록, 아이템이 여러개인 경우 공백 없이 콤마(,)로 구분하여 입력] 

*similThreshold : 추천에 사용되는 최소 유사도 0~1의 실수 값
*[출력 데이터 폴더 경로]/recomResult.txt : 추천 결과 파일

수행 예제:
hadoop jar ankus-core2-userbased-Recommend-1.1.0.jar \
UserBased_Recommendation \
-input /data/user_sim.txt \
-output /result/recommend \
-similPath /result/user_sim_result/ \
-delimiter :: \
-uidIndex 0 \
-iidIndex 1 \
-ratingIndex 2  \
-basedType user \
-similDelimiter :: \
-similThreshold 0.2 \
-targetUID 1000\
 
*입력 예제
1::1193::5::978300760
1::661::3::978302109
1::914::3::978301968
1::3408::4::978300275
1::2355::5::978824291
1::1197::3::978302268
1::1287::5::978302039
1::2804::5::978300719
1::594::4::978302268
1::919::4::978301368
.....
*모델 예제
....
1000::101::6.325
1000::1010::11.358
1000::1014::6.0
1000::1015::7.0
1000::1017::8.602
1000::1018::4.899
1000::1019::11.489
1000::102::6.164
1000::1020::2.236
1000::1021::2.646
1000::1029::10.954
1000::103::3.606
1000::1030::6.164
....

*추천 결과
3233::5.0::1
3607::5.0::1
3881::5.0::1
53::5.0::4
557::5.0::1
572::5.0::1
787::5.0::2
3245::4.8::5
2503::4.75::8
3338::4.609::23
