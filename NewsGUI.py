import sys
import time
import requests
from PyQt5 import QtWidgets
from PyQt5.QtCore import *
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtGui import QColor
from GetNews import GetNews, GetDisclosure

# Thread Constants
FIRST_GET_NEWS_NUM = 70  # 처음 받아오는 뉴스개수
DELETE_NUM = 10  # db 에서 한번에 지울 개수
DELETE_LIMIT = 80  # db 행 몇개 이상일때 지울지 개수
TIME_WAIT_NEWS = 3  # 뉴스 새로고침 시간
TIME_WAIT_DISCLOSURE = 3  # 공시 새로고침 시간
TIME_WAIT = 0.5  # 업데이트 안할 경우 wait time
MAX_LIST_NUM = 13  # 뉴스 받아오는 개수를 다르게 설정 -> 계속 같으면 업데이트 안되는 경우 발생

# Gui Constants
TABLE_ROW_LIMIT = 300  # 테이블 행 몇개 이상일 때 지울지
TABLE_DELETE_TO = 250  # 몇 행까지 남길지


class DisclosureThread(QThread):
    get_disclosure_signal = pyqtSignal(dict)  # 공시업데이트 pyqt 시그널 정의

    def __init__(self, db_path):
        super().__init__()
        self.disclosure = GetDisclosure(db_path)
        self.disclosure.create_tables()
        self.disclosure.delete_all_values()

        self.is_run = True

    def run(self):
        while True:
            if self.is_run:
                d_list = self.disclosure.check_new_disclosure()
                self.get_disclosure_signal.emit(d_list)  # 공시 리스트 딕셔너리 emit
                self.disclosure.put_list_to_db(d_list)
                self.disclosure.delete_old_data(DELETE_NUM, DELETE_LIMIT)

                time.sleep(TIME_WAIT_DISCLOSURE)
            else:
                time.sleep(TIME_WAIT)


class NewsThread(QThread):  # 뉴스업데이트를 위한 Q 쓰레드를 상속받는 클래스
    get_news_signal = pyqtSignal(dict)  # 뉴스업데이트 pyqt 시그널 정의

    def __init__(self, db_path):
        super().__init__()
        self.get_news = GetNews(db_path)
        self.get_news.create_tables()
        self.get_news.delete_all_values()

        self.is_run = True
        self.check_news_num = 1

    def run(self):
        while True:
            if self.is_run:
                news_list = self.get_news.check_new_news(self.check_news_num)
                self.get_news_signal.emit(news_list)  # 뉴스 리스트 딕셔너리 emit
                self.get_news.put_list_to_db(news_list)
                self.get_news.delete_old_data(DELETE_NUM, DELETE_LIMIT)

                self.check_news_num += 1
                if self.check_news_num > MAX_LIST_NUM:
                    self.check_news_num = 1

                time.sleep(TIME_WAIT_NEWS)
            else:
                time.sleep(TIME_WAIT)


class Gui(QtWidgets.QWidget):  # Table 위젯과 WebEngine 위젯을 포함하는 pyqt 클래스
    def __init__(self, db_path1, db_path2):
        super().__init__()
        print('Creating Layout...')
        # Layout
        self.setWindowTitle('Finance')
        self.grid = QtWidgets.QGridLayout()
        self.setLayout(self.grid)
        self.resize(1870, 850)
        self.center()

        # Btn
        self.btn1 = QtWidgets.QPushButton('running', self)
        self.btn1.clicked.connect(self.push_btn)

        # WebEngine
        self.browser = QWebEngineView()
        self.browser.setMaximumSize(1200, 1080)

        # Table
        self.table = QtWidgets.QTableWidget(self)
        self.table.setMaximumSize(670, 1080)
        self.table.setAutoScroll(False)

        self.table.setRowCount(0)
        self.table.setColumnCount(7)
        self.table.setColumnWidth(0, 400)
        self.table.setColumnWidth(1, 60)
        self.table.setColumnWidth(2, 60)
        self.table.setColumnWidth(3, 100)
        self.table.setColumnWidth(4, 60)
        self.table.setColumnWidth(5, 60)
        self.table.setColumnWidth(6, 10)

        self.table.setHorizontalHeaderLabels(
            ['Title', 'Displayed', 'Created', 'Category', 'Summary', 'Date', 'NewsId']
        )
        self.table.cellClicked.connect(self.click_news)

        print('Setting Thread...')
        # Thread 생성
        self.news_thread = NewsThread(db_path1)
        self.disclosure_thread = DisclosureThread(db_path2)
        self.init_table()  # 처음에만 테이블에 정보 넣기

        # Thread 시그널 생성 및 시작
        self.news_thread.get_news_signal.connect(self.update_table)
        self.disclosure_thread.get_disclosure_signal.connect(self.update_table2)
        self.news_thread.start()
        self.disclosure_thread.start()

        self.is_run = True

        # Grid
        self.grid.addWidget(self.table, 0, 0)
        self.grid.addWidget(self.browser, 0, 1)
        self.grid.addWidget(self.btn1, 1, 0, 1, 2)

        print('All Completed')
        self.show()

    def center(self):  # 창 가운데로 설정
        qr = self.frameGeometry()
        cp = QtWidgets.QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

    @pyqtSlot()
    def push_btn(self):  # 쓰레드 시작 / 정지 slot 함수
        if self.news_thread.is_run and self.disclosure_thread.is_run:
            self.news_thread.is_run = False
            self.disclosure_thread.is_run = False
        else:
            self.news_thread.is_run = True
            self.disclosure_thread.is_run = True

    def init_table(self):
        # 공시크롤링
        init_d_list = self.disclosure_thread.disclosure.get_new_data()
        self.disclosure_thread.disclosure.put_list_to_db(init_d_list)

        # 뉴스크롤링
        init_news = self.news_thread.get_news.get_page_list(FIRST_GET_NEWS_NUM)
        self.news_thread.get_news.put_list_to_db(init_news)

        # 테이블 삽입
        self.put_news_to_table(init_news)
        self.put_d_list_to_table(init_d_list)

        # 정렬
        self.table.sortItems(5, Qt.DescendingOrder)
        self.table.selectRow(0)
        self.click_news()

    def put_news_to_table(self, news_list):  # 테이블에 뉴스크롤링 정보 삽입
        for category in self.news_thread.get_news.categories:
            for data in reversed(news_list[category]):
                if data is not None:
                    now = time.localtime()
                    now_time = f'{now.tm_hour:02}:{now.tm_min:02}:{now.tm_sec:02}'
                    self.table.insertRow(0)
                    self.table.setItem(0, 0, QtWidgets.QTableWidgetItem(data['title']))
                    self.table.setItem(0, 1, QtWidgets.QTableWidgetItem(now_time))
                    self.table.setItem(0, 2, QtWidgets.QTableWidgetItem(data['createdAt'].split(' ')[-1]))
                    self.table.setItem(0, 3, QtWidgets.QTableWidgetItem(category))
                    self.table.setItem(0, 4, QtWidgets.QTableWidgetItem(data['summary']))
                    self.table.setItem(0, 5, QtWidgets.QTableWidgetItem(data['createdAt']))
                    self.table.setItem(0, 6, QtWidgets.QTableWidgetItem(data['newsId']))
                    self.table.setRowHeight(0, 30)
                    self.table.scrollToTop()

    def put_d_list_to_table(self, d_list):  # 테이블에 공시크롤링 정보 삽입
        for category in self.disclosure_thread.disclosure.categories:
            for data in reversed(d_list[category]):
                if data is not None:
                    now = time.localtime()
                    now_time = f'{now.tm_hour:02}:{now.tm_min:02}:{now.tm_sec:02}'
                    self.table.insertRow(0)
                    self.table.setItem(0, 0, QtWidgets.QTableWidgetItem(data['title']))
                    self.table.setItem(0, 1, QtWidgets.QTableWidgetItem(now_time))
                    self.table.setItem(0, 2, QtWidgets.QTableWidgetItem(data['time']))
                    self.table.setItem(0, 3, QtWidgets.QTableWidgetItem(data['company']))
                    self.table.setItem(0, 4, QtWidgets.QTableWidgetItem(data['market']))
                    self.table.setItem(0, 5, QtWidgets.QTableWidgetItem(data['date']))
                    self.table.setItem(0, 6, QtWidgets.QTableWidgetItem(data['report']))
                    self.table.setRowHeight(0, 30)
                    self.table.item(0, 0).setBackground(QColor(100, 255, 0))
                    self.table.item(0, 3).setBackground(QColor(100, 255, 0))
                    self.table.scrollToTop()

    @pyqtSlot(dict)
    def update_table(self, news_list):  # 뉴스 정보 딕셔너리를 받아 테이블 업데이트
        self.btn1.setText('Received News List')
        self.put_news_to_table(news_list)

        row_count = self.table.rowCount()
        if row_count > TABLE_ROW_LIMIT:
            for i in reversed(range(TABLE_DELETE_TO, row_count)):
                self.table.removeRow(i)

    @pyqtSlot(dict)
    def update_table2(self, d_list):  # 공시 정보 업데이트
        self.btn1.setText('Received Disclosure List')
        self.put_d_list_to_table(d_list)

        row_count = self.table.rowCount()
        if row_count > TABLE_ROW_LIMIT:
            for i in reversed(range(TABLE_DELETE_TO, row_count)):
                self.table.removeRow(i)

    @pyqtSlot()
    def click_news(self):  # 뉴스를 클릭할 경우 웹브라우저에 내용 표시
        current_row = self.table.currentRow()
        self.table.selectRow(current_row)

        news_id = self.table.item(current_row, 6).text()
        if news_id[:4] == 'http':
            self.browser.setUrl(QUrl(news_id))
        else:
            try:
                res = requests.get('https://finance.daum.net/content/news/'+news_id,
                                   headers=self.news_thread.get_news.header)
            except Exception as e:
                print(e)
            json = res.json()
            html = '<h3>'+json['title']+'</h3>\n'+'<div>'+json['createdAt']+'</div>'+json['content']
            self.browser.setHtml(html)

