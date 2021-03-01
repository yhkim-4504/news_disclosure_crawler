import requests
import sqlite3
import time
from bs4 import BeautifulSoup as Soup


class GetNews:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self.cur = self.conn.cursor()
        self.header = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/87.0.4280.66 Safari/537.36',
            'referer': 'https://finance.daum.net/news',
        }
        self.param = {
            'category': '',
            'perPage': 10,
            'page': 1
        }
        self.news_url = 'https://finance.daum.net/content/news'
        self.categories = ['stock', 'market_condition', 'stock_general', 'company']
        self.create_tables()

    def create_tables(self):
        for category in self.categories:
            self.cur.execute(
                f'CREATE TABLE IF NOT EXISTS {category}(newsID TEXT, title TEXT, summary TEXT, \
                keywords TEXT, createdAt TEXT)')

    def delete_all_tables(self):
        for category in self.categories:
            self.cur.execute(f'DROP TABLE {category}')

    def delete_all_values(self):
        for category in self.categories:
            self.cur.execute(f'DELETE FROM {category}')

    def get_recent_data(self, category, limit):
        recent_list = self.cur.execute(f'SELECT newsID FROM {category} ORDER BY createdAt DESC LIMIT {limit}')

        return [x[0] for x in recent_list]

    def get_all_data(self, category):
        all_data = self.cur.execute(f'SELECT newsID FROM {category}')

        return [x[0] for x in all_data]

    def db_close(self):
        self.conn.close()

    def get_page_list(self, list_num):
        self.param['perPage'] = list_num
        dic = {}
        for category in self.categories:
            self.param['category'] = category
            while True:
                try:
                    res = requests.get(self.news_url, headers=self.header, params=self.param, timeout=2)
                    break
                except Exception as e:
                    print(e)
                    continue
            dic[category] = res.json()['data']
            if res.status_code != 200:
                print('Status Error :', res.status_code)
                raise Exception('RESPONSE ERROR')

        return dic

    def delete_old_data(self, delete_num, limit_num):
        for category in self.categories:
            self.cur.execute(f'DELETE from {category} where (select count(*) from {category}) > {limit_num} \
            AND createdAt <= \
            (select max(createdAt) FROM (select createdAt from {category} order by createdAt ASC limit {delete_num}))')

    def put_list_to_db(self, news_list):
        for category in self.categories:
            for data in reversed(news_list[category]):
                if data is not None:
                    self.cur.execute(
                        f'INSERT INTO {category} VALUES(?, ?, ?, ?, ?)',
                        (data['newsId'], data['title'], data['summary'], ' '.join(data['keywords']), data['createdAt'])
                    )

    def check_new_news(self, list_num):
        first_news = self.get_page_list(list_num)
        for category in self.categories:
            all_data = self.get_all_data(category)

            for i in range(len(first_news[category])):
                if first_news[category][i]['newsId'] not in all_data:
                    now = time.localtime()
                    print(f'\n{now.tm_hour:02}:{now.tm_min:02}:{now.tm_sec:02}', first_news[category][i]['createdAt'],
                          category, '[', first_news[category][i]['title'], ']')
                    print(first_news[category][i]['summary'])
                    # print(first_news[category][i]['newsId'])
                else:
                    first_news[category][i] = None
        return first_news

    def __del__(self):
        self.conn.close()


class GetDisclosure:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self.cur = self.conn.cursor()
        self.error_occured = False
        self.header = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/87.0.4280.66 Safari/537.36',
        }
        self.kind_param = {
            'method': 'searchTodayDisclosureSub',
            'currentPageSize': 15,
            'pageIndex': 1,
            'orderMode': 0,
            'orderStat': 'D',
            'forward': 'todaydisclosure_sub',
        }
        self.dart = 'http://dart.fss.or.kr/dsac001/search.ax?&mdayCnt=0'
        self.kind = 'https://kind.krx.co.kr/disclosure/todaydisclosure.do'
        self.categories = ['dart', 'kind']
        self.create_tables()

    def create_tables(self):
        self.cur.execute(
            f'CREATE TABLE IF NOT EXISTS disclosure(market TEXT, time TEXT, company TEXT, \
            title TEXT, date TEXT, report TEXT)')

    def delete_all_tables(self):
        self.cur.execute(f'DROP TABLE disclosure')

    def delete_all_values(self):
        self.cur.execute(f'DELETE FROM disclosure')

    def get_all_data(self):
        all_data = list(self.cur.execute(f'SELECT time, company FROM disclosure'))

        return all_data

    def db_close(self):
        self.conn.close()

    def get_new_data(self):
        dic = {}
        dic[self.categories[0]] = self.get_dart_data()
        dic[self.categories[1]] = self.get_kind_data()

        return dic

    def get_dart_data(self):
        while True:
            try:
                res = requests.get(self.dart, timeout=2)
                if res.status_code != 200:
                    raise Exception('status not 200')
                else:
                    break
            except Exception as e:
                print(e)
                self.error_occured = True
                continue

        dart_data = Soup(res.text, 'html.parser')
        dart_list = []
        for data in dart_data.select('table')[0].select('tr')[1:]:
            if data.select('img') == []:
                continue
            market = data.select('img')[0].attrs['title']
            if (market != '기타법인') and (market != '코넥스시장'):
                dic = {}
                created_time = data.select('td')[0].text.strip() + ':00'
                date = data.select('td')[4].text.strip().split('.')
                date = f'{date[0]:0>2}-{date[1]:0>2}-{date[2]:0>2} {created_time}'
                dic['market'] = market
                dic['time'] = created_time
                dic['company'] = data.select('td')[1].text.strip()
                dic['title'] = data.select('td')[2].text.strip().replace('\r', ''). \
                    replace('\t', '').replace('\n', '').replace('  ', '')
                dic['date'] = date
                dic['report'] = 'http://dart.fss.or.kr/' + data.select('a')[-1].attrs['href']
                dart_list.append(dic)

        return dart_list

    def get_kind_data(self):
        while True:
            try:
                res = requests.get(self.kind, params=self.kind_param, timeout=2)
                if res.status_code != 200:
                    raise Exception('status not 200')
                else:
                    break
            except Exception as e:
                print(e)
                self.error_occured = True
                continue
        kind_data = Soup(res.text, 'html.parser')

        kind_list = []
        for data in kind_data.select('#parkman'):
            if data.select('td')[1].select('img') == []:
                continue
            market = data.select('td')[1].select('img')[0].attrs['alt']
            if market != '코넥스':
                dic = {}
                report_num = data.select('td')[2].select('a')[0].attrs['onclick'].split("'")[1]
                date = report_num[:8]
                created_time = data.select('td')[0].text.strip() + ':00'
                date = f'{date[:4]:0>2}-{date[4:6]:0>2}-{date[6:]:0>2} {created_time}'
                dic['market'] = market
                dic['time'] = created_time
                dic['company'] = data.select('td')[1].text.strip()
                dic['title'] = data.select('td')[2].text.strip()
                dic['date'] = date
                dic['report'] = 'https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno=' + \
                                data.select('td')[2].select('a')[0].attrs['onclick'].split("'")[1]
                kind_list.append(dic)

        return kind_list

    def delete_old_data(self, delete_num, limit_num):
        self.cur.execute(f'DELETE from disclosure where (select count(*) from disclosure) > {limit_num} \
        AND time <= \
        (select max(time) FROM (select time from disclosure order by time ASC limit {delete_num}))')

    def put_list_to_db(self, dis_list):
        for category in self.categories:
            for data in reversed(dis_list[category]):
                if data is not None:
                    self.cur.execute(
                        'INSERT INTO disclosure VALUES(?, ?, ?, ?, ?, ?)',
                        (data['market'], data['time'], data['company'], data['title'], data['date'], data['report'])
                    )

    def check_new_disclosure(self):
        print('\r1', end='')
        d_list = self.get_new_data()
        print('\r2', end='')
        all_data = self.get_all_data()
        for category in self.categories:
            for i in range(len(d_list[category])):
                if (d_list[category][i]['time'], d_list[category][i]['company']) not in all_data:
                    now = time.localtime()
                    # print(f'\n{now.tm_hour:02}:{now.tm_min:02}:{now.tm_sec:02}', d_list[category][i]['title'])
                else:
                    d_list[category][i] = None

        return d_list

    def __del__(self):
        self.conn.close()