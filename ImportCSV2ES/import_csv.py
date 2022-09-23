from elasticsearch import helpers, Elasticsearch
import csv


es = Elasticsearch(['http://elasticsearch-server-ip:9200/'], port=9200)

if es.indices.exists(index="news-bulk"):
  pass
else:
  es.indices.create(index="news-bulk")

with open('D:/IT/Project/KoreaNewsCrawler-master/output/Article_IT과학_20200101_20200131.csv', encoding='UTF8') as f:
  reader = csv.DictReader(f)
  helpers.bulk(es, reader, index="news-bulk", raise_on_error=False)