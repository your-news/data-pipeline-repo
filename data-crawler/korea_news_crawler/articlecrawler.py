#!/usr/bin/env python3
# -*- coding: utf-8, euc-kr -*-
import os
import platform
import calendar
import requests
from time import sleep
from bs4 import BeautifulSoup
from multiprocessing import Process
from exceptions import InvalidDay, InvalidYear, InvalidCategory, InvalidMonth, OverbalanceMonth, \
    OverbalanceDay, ResponseTimeout
from articleparser import ArticleParser
from writer import Writer
from logwriter import LogWriter
import datetime
import sys
import boto3
import socket
from pathlib import Path

[sys.path.append(i) for i in ['.', '..']]


class ArticleCrawler(object):

    def __init__(self):
        # 생활문화 -> 문화, 세계 -> 국제
        self.categories = {'정치': 100, '경제': 101, '사회': 102, '문화': 103, '국제': 104, 'IT과학': 105}
        self.selected_categories = []
        self.date = {'start_year': 0, 'start_month': 0, 'start_day': 0, 'end_year': 0, 'end_month': 0, 'end_day': 0}
        self.user_operating_system = str(platform.system())

    def set_category(self, *args):
        for key in args:
            if self.categories.get(key) is None:
                raise InvalidCategory(key)
        self.selected_categories = args

    def set_date_range(self, start_date: str, end_date: str):
        start = list(map(int, start_date.split("-")))
        end = list(map(int, end_date.split("-")))

        # Setting Start Date
        if len(start) == 1:  # Input Only Year
            start_year = start[0]
            start_month = 1
            start_day = 1
        elif len(start) == 2:  # Input Year and month
            start_year, start_month = start
            start_day = 1
        elif len(start) == 3:  # Input Year, month and day
            start_year, start_month, start_day = start

        # Setting End Date
        if len(end) == 1:  # Input Only Year
            end_year = end[0]
            end_month = 12
            end_day = 31
        elif len(end) == 2:  # Input Year and month
            end_year, end_month = end
            end_day = calendar.monthrange(end_year, end_month)[1]
        elif len(end) == 3:  # Input Year, month and day
            end_year, end_month, end_day = end

        args = [start_year, start_month, start_day, end_year, end_month, end_day]

        if start_year > end_year:
            raise InvalidYear(start_year, end_year)
        if start_month < 1 or start_month > 12:
            raise InvalidMonth(start_month)
        if end_month < 1 or end_month > 12:
            raise InvalidMonth(end_month)
        if start_day < 1 or calendar.monthrange(start_year, start_month)[1] < start_day:
            raise InvalidDay(start_day)
        if end_day < 1 or calendar.monthrange(end_year, end_month)[1] < end_day:
            raise InvalidDay(end_day)
        if start_year == end_year and start_month > end_month:
            raise OverbalanceMonth(start_month, end_month)
        if start_year == end_year and start_month == end_month and start_day > end_day:
            raise OverbalanceDay(start_day, end_day)

        for key, date in zip(self.date, args):
            self.date[key] = date
        print(str(sys.argv[1]), self.date)

    @staticmethod
    def make_news_page_url(category_url, date):
        made_urls = []
        for year in range(date['start_year'], date['end_year'] + 1):
            if date['start_year'] == date['end_year']:
                target_start_month = date['start_month']
                target_end_month = date['end_month']
            else:
                if year == date['start_year']:
                    target_start_month = date['start_month']
                    target_end_month = 12
                elif year == date['end_year']:
                    target_start_month = 1
                    target_end_month = date['end_month']
                else:
                    target_start_month = 1
                    target_end_month = 12

            for month in range(target_start_month, target_end_month + 1):
                if date['start_month'] == date['end_month']:
                    target_start_day = date['start_day']
                    target_end_day = date['end_day']
                else:
                    if year == date['start_year'] and month == date['start_month']:
                        target_start_day = date['start_day']
                        target_end_day = calendar.monthrange(year, month)[1]
                    elif year == date['end_year'] and month == date['end_month']:
                        target_start_day = 1
                        target_end_day = date['end_day']
                    else:
                        target_start_day = 1
                        target_end_day = calendar.monthrange(year, month)[1]

                for day in range(target_start_day, target_end_day + 1):
                    if len(str(month)) == 1:
                        month = "0" + str(month)
                    if len(str(day)) == 1:
                        day = "0" + str(day)

                    # 날짜별로 Page Url 생성
                    url = category_url + str(year) + str(month) + str(day)

                    # totalpage는 네이버 페이지 구조를 이용해서 page=10000으로 지정해 totalpage를 알아냄
                    # page=10000을 입력할 경우 페이지가 존재하지 않기 때문에 page=totalpage로 이동 됨 (Redirect)
                    totalpage = ArticleParser.find_news_totalpage(url + "&page=10000")
                    for page in range(1, totalpage + 1):
                        made_urls.append(url + "&page=" + str(page))
        return made_urls

    @staticmethod
    def get_url_data(url, max_tries=5):
        remaining_tries = int(max_tries)
        while remaining_tries > 0:
            try:
                return requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            except requests.exceptions:
                sleep(1)
            remaining_tries = remaining_tries - 1
        raise ResponseTimeout()

    def crawling(self, section):

        s3 = boto3.client('s3',
                          region_name='ap-northeast-2',
                          aws_access_key_id='AKIAQSZOT42WYGBLNIWR',
                          aws_secret_access_key='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')

        # Multi Process PID
        print(section + " PID: " + str(os.getpid()))

        writer = Writer(category='news', article_category=section, date=self.date)
        writer.write_row(["date", "section", "press", "author", "title", "contents",
                          "imageUrl", "url"])

        log_writer = LogWriter(category='news', article_category=section, date=self.date)

        # 기사 url 형식
        url_format = f'http://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1={self.categories.get(section)}&date='
        # start_year년 start_month월 start_day일 부터 ~ end_year년 end_month월 end_day일까지 기사를 수집합니다.
        target_urls = self.make_news_page_url(url_format, self.date)
        print(f'{section} Urls are generated')

        print(f'{section} is collecting ...')
        i = 0
        acc = 0
        for url in target_urls:

            j = 0

            request = self.get_url_data(url)
            document = BeautifulSoup(request.content, 'html.parser')

            # 각 페이지에 있는 기사들 가져오기
            temp_post = document.select('.newsflash_body .type06_headline li dl')
            temp_post.extend(document.select('.newsflash_body .type06 li dl'))

            # 각 페이지에 있는 기사들의 url 저장
            post_urls = []
            for line in temp_post:
                # 해당되는 page에서 모든 기사들의 URL을 post_urls 리스트에 넣음
                post_urls.append(line.a.get('href'))
            del temp_post

            for content_url in post_urls:  # 기사 url
                # 크롤링 대기 시간
                sleep(0.01)

                # 기사 HTML 가져옴
                request_content = self.get_url_data(content_url)

                try:
                    document_content = BeautifulSoup(request_content.content, 'html.parser')
                except:
                    continue
                try:
                    # 기사 원문url 가져옴
                    origin_url = document_content.find_all('a', {'class': 'media_end_head_origin_link'})[0]['href']

                    # 기사 제목 가져옴
                    tag_headline = document_content.find_all('h2', {'class': 'media_end_head_headline'})
                    # 뉴스 기사 제목 초기화
                    title = ''
                    title = title + ArticleParser.clear_headline(str(tag_headline[0].find_all(text=True)))

                    # 기사 본문 가져옴
                    try:
                        element1 = document_content.find('em', {'class': 'img_desc'})
                        element1.decompose()
                        element2 = document_content.find('strong', {'class': 'media_end_summary'})
                        element2.decompose()
                        tag_content = document_content.find_all('div', {'id': 'dic_area'})
                    except:
                        tag_content = document_content.find_all('div', {'id': 'dic_area'})

                    cleansoup = BeautifulSoup(str(tag_content[0]).replace("<br>", "\n").replace("<br/>", "\n"),
                                              'html.parser')

                    # 뉴스 기사 본문 초기화
                    contents = ''
                    contents = contents + ArticleParser.clear_content(str(cleansoup.find_all(text=True)))

                    try:
                        # 기사 언론사 가져옴
                        tag_content = document_content.find_all('meta', {'property': 'og:article:author'})
                        # 언론사 초기화
                        press = ''
                        press = press + tag_content[0]['content'].split("|")[0]
                        press = press.replace(" ", "")
                    except:
                        press = ""

                    # 기자 이름 가져옴
                    try:
                        tag_author = document_content.find_all('em', {'class': 'media_end_head_journalist_name'})
                        # 뉴스 기자 이름 초기화
                        author = ''
                        author = author + ArticleParser.clear_headline(
                            str(tag_author[0].find_all(text=True)).replace(" 기자", ""))
                    except:
                        try:
                            tag_author = document_content.find_all('span', {'class': 'byline_s'})
                            author = ''
                            author = author + ArticleParser.clear_headline(
                                str(tag_author[0].find_all(text=True)))[:3]
                        except:
                            author = ""

                    ## 뉴스 기사 사진 가져옴
                    try:
                        image_url = document_content.find_all('img', {'id': 'img1'})[0]['data-src']
                    except:
                        image_url = ''

                    # 기사 시간대 가져옴
                    time = document_content.find_all('span', {
                        'class': "media_end_head_info_datestamp_time _ARTICLE_DATE_TIME"})[0]['data-date-time'].replace(
                        " ", "T")

                    # CSV 작성
                    writer.write_row([time, section, press, author, title, contents,
                                      image_url, origin_url])

                    i += 1
                    j += 1
                    now = datetime.datetime.now()

                    # 데이터 100개 단위마다 LOG CSV 작성
                    if i % 100 == 0:
                        print(i, acc, now.strftime("%m/%d, %H:%M:%S"), time.split("T")[0])
                        log_writer.write_row([i, acc, now.strftime("%m/%d, %H:%M:%S"), time.split("T")[0]])
                    else:
                        continue

                    del time
                    del press, contents, title, author, image_url
                    del tag_content, tag_headline, origin_url
                    del request_content, document_content

                # UnicodeEncodeError
                except Exception as ex:
                    del request_content, document_content
                    pass
        writer.close()
        log_writer.close()

        # 크롤링 파일 이름 불러오기
        files_path = "../output/"
        file_name_and_time_list = []
        for f_name in os.listdir(f"{files_path}"):
            written_time = os.path.getctime(f"{files_path}{f_name}")
            file_name_and_time_list.append((f_name, written_time))
        sorted_file_list = sorted(file_name_and_time_list, key=lambda x: x[1], reverse=True)
        recent_file = sorted_file_list[0]
        recent_file_name = recent_file[0]

        # 로그 파일 이름 불러오기
        files_path = "../log_output/"
        file_name_and_time_list = []
        for f_name in os.listdir(f"{files_path}"):
            written_time = os.path.getctime(f"{files_path}{f_name}")
            file_name_and_time_list.append((f_name, written_time))
        sorted_file_list = sorted(file_name_and_time_list, key=lambda x: x[1], reverse=True)
        recent_file = sorted_file_list[0]
        log_recent_file_name = recent_file[0]

        # 크롤링 파일 s3 bucket에 전송
        s3.upload_file(f'../output/{recent_file_name}', 'yournewsbucket', f'{socket.gethostname()}/{recent_file_name}')

        # 완료된 로그 파일, 크롤링 파일 위치 변경
        completion_path = f'../completion'
        if os.path.exists(completion_path) is not True:
            os.mkdir(completion_path)

        Path(f"../output/{recent_file_name}").rename(f"../completion/{recent_file_name}")
        Path(f"../log_output/{log_recent_file_name}").rename(f"../completion/{log_recent_file_name}")

        print(f'{recent_file_name} crawling is finish...')
        print(f'{recent_file_name} crawling is finish...')
        print(f'{recent_file_name} crawling is finish...')


    def start(self):
        # MultiProcess 크롤링 시작
        for category_name in self.selected_categories:
            proc = Process(target=self.crawling, args=(category_name,))
            proc.start()

if __name__ == "__main__":
    Crawler = ArticleCrawler()
    # elf.categories = {'정치': 100, '경제': 101, '사회': 102, '문화': 103, '국제': 104, 'IT과학': 105}
    Crawler.set_category(str(sys.argv[1]))
    # Crawler.set_category('정치')
    Crawler.set_date_range(str(sys.argv[2]), str(sys.argv[3]))
    # Crawler.set_date_range('2018-1-1', '2018-1-1')
    Crawler.start()
