from elasticsearch import helpers, Elasticsearch
import os
import csv
import time
import ctypes as ct

es = Elasticsearch(['http://elasticsearch-server-ip:9200/'], port=9200)

# csv 파일 디렉토리 변수 초기화
dir_path = "E:\\news2"

# csv field 에러 처리를 위한 limit 사이즈 확장
csv.field_size_limit(int(ct.c_ulong(-1).value // 2))

# Bulk data 삽입 시간 측정
before = round(time.time())
print("데이터 삽입 전 시간 :", time.strftime('%c', time.localtime(before)))
print("데이터 삽입 전 시간(초) :", before)

i = 0 # 파일 갯수 확인용 변수
# 디렉토리 내부 순환
for (root, directories, files) in os.walk(dir_path):
    # csv 파일 순환
    for file in files:
        # csv 파일 명 쪼개어 index 명으로 변경
        # news_IT과학_20140201_20140228 -> news-IT과학-201402
        file_path = os.path.join(root, file).split("_")
        indexname = "news-" + file_path[1] + "-" + file_path[2][0:6]
        print(indexname)

        # ES 내부 index 존재 여부 확인
        if es.indices.exists(index=indexname):
            pass
        else:
            es.indices.create(index=indexname)
        # csv파일 열어 Bulk insert
        with open(file_path, encoding='UTF8') as f:
            reader = csv.DictReader(f)
            helpers.bulk(es, reader, index=indexname, raise_on_error=False)
        # 파일 당 삽입 소요시간 측정
        i += 1
        print(i,"월 종료 소요시간 : ",(round(time.time())-before), "s")

# 최종 소요시간 측정
after = round(time.time())
print("데이터 삽입 전 시간 :", time.strftime('%c', time.localtime(after)))
print("데이터 삽입 전 시간(초) :", after)
print("dif : ", (after-before), "s")