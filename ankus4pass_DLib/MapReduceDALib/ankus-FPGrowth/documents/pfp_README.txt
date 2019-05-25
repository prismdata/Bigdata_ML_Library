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