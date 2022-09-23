from elasticsearch import helpers, Elasticsearch
import os
import csv
import time
import ctypes as ct


es = Elasticsearch(['http://elasticsearch-server-ip:9200/'], port=9200)

# csv 파일 디렉토리 변수 초기화
dir_path = "E:\\news2"

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
