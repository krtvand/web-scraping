#!/usr/bin/env python
# -*- coding: utf-8 -*-

REESTR_OBORON_PREDPR = 'reestr_oboron_predpr.xls'
PROXY_LIST = 'proxy_list'

import re
import logging
import sys
import time
import csv
from socket import gethostbyname
from multiprocessing import Pool

import xlrd
from grab import Grab
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
#logging.basicConfig(level=logging.DEBUG)


# Зададим параметры логгирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(u'%(filename)s[LINE:%(lineno)d]# '
                              u'%(levelname)-8s [%(asctime)s]  %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


Base = declarative_base()

# Подключение к базе данных
engine = create_engine('mysql://root:8-9271821473@localhost/oboron_predpr')
session = sessionmaker()
session.configure(bind=engine)


class Google_search_resaults(Base):
    __tablename__ = 'google_search_resaults'
    id = Column(String(10), primary_key=True)
    request = Column(String(1000))
    domain1 = Column(String(1000))
    ip1 = Column(String(100))
    country1 = Column(String(100))
    title1 = Column(String(1000))
    domain2 = Column(String(1000))
    ip2 = Column(String(100))
    country2 = Column(String(100))
    title2 = Column(String(1000))
    domain3 = Column(String(1000))
    ip3 = Column(String(100))
    country3 = Column(String(100))
    title3 = Column(String(1000))

    def __repr__(self):
        return "<Google_search_resaults('%s','%s', '%s')>" % \
               (self.request, self.domain1, self.title1)

class Contacts(Base):
    __tablename__ = 'oboron_predpr_contacts'
    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(String(1000))
    contacts_link = Column(String(1000))
    email = Column(String(1000))


Base.metadata.create_all(engine)

class Google(object):
    def __init__(self, proxy=False, connect_timeout=3):
        self.g = Grab()
        self.cites = []
        self.domains = []
        self.titles = []
        self.g.setup(log_file='/scripts/log_google_parser.html')
        self.g.setup(debug_post=True)
        self.g.setup(follow_location=True)
        self.g.setup(connect_timeout=connect_timeout)
        if proxy:
            with open(PROXY_LIST,'r') as proxies_file:
                self.g.proxylist.load_list(proxies_file)
                self.g.proxylist.get_random_proxy()
                self.g.setup(proxy_auto_change=False)
                print '!!!!'
    def __is_captcha(self):
        return self.g.doc.select('//body/div/form[@action="CaptchaRedirect"]').exists()
    def search(self, search_request, count=10):
        try:
            self.g.go('https://google.ru')
            self.g.doc.set_input('q', search_request)
            resp = self.g.doc.submit(submit_name='btnK')
        except Exception as e:
            logger.critical('Google request failed %s: %s %s' % (search_request, e.message, e.args))
            return False
        if self.__is_captcha():
            logger.warning('Google return captcha')
            return False
        self.parsegooglepage()
        while len(self.cites) < count:
            try:
                self.g.go(self.g.doc.select('//a[@id="pnnext"]').attr('href'))
            except:
                logger.critical('Google next button fails')
                return False
            self.parsegooglepage()
        self.domains += [re.sub(r'(^https?://)?([\s/].*)?','',x) for x in self.cites]
        return True

    def parsegooglepage(self):
        tencites = self.g.doc.select('//div[@class="g"]//cite')
        if len(tencites) == 0:
            self.cites += [u'XPath request for cite returned NULL' for x in range(10)]
        else:
            for it in tencites:
                if type(it.text()) is str:
                    cite = it.text().decode('utf-8')
                    self.cites.append(cite)
                else:
                    self.cites.append(it.text())
        tentitles = self.g.doc.select('//h3[@class="r"]')
        if len(tentitles) == 0:
            self.titles += [u'XPath request for title returned NULL' for x in range(10)]
        else:
            for it in tentitles:
                if type(it.text()) is str:
                    title = it.text().decode('utf-8')
                    self.titles.append(title)
                else:
                    self.titles.append(it.text())

    def getCites(self, count=10):
        return self.cites[:count]

def read_reestr_from_excel():
        try:
            rb = xlrd.open_workbook(REESTR_OBORON_PREDPR)
        except:
            logger.critical('Can not open price file!')
            return
        google_search_resault = Google_search_resaults()
        sheet = rb.sheet_by_index(0)
        s = session()
        for row_index in range(sheet.nrows):
            # Сохраняем артикул в нужном формате, и пропускаем строку,
            # если нет записи для артикула
            id = sheet.row(row_index)[0].value
            logger.debug('id %s' % id)
            if isinstance(id, float):
                google_search_resault.id = ('%04.0f' % id)
            else:
                continue
            request = sheet.row(row_index)[2].value
            # Удаляем пробельные символы в начале и в кноце строки
            request = request.lstrip().rstrip()
            google_search_resault.request = request.encode('utf-8')
            s.merge(google_search_resault)
        s.commit()

def googling():
    s = session()
    for request in s.query(Google_search_resaults).all():
        logger.info('request %s %s' % (request.id, request.request.decode('utf-8')))
        if request.domain1 is None:
            goo = Google(proxy=True)
            if goo.search(request.request.decode('utf-8'), count=3) is False:
                logger.critical('Google request failed')
                return False
                time.sleep(10)
                continue
            else:
                logger.debug(goo.domains[0])
                request.domain1 = goo.domains[0].encode('utf-8')
                request.domain2 = goo.domains[1].encode('utf-8')
                request.domain3 = goo.domains[2].encode('utf-8')
                request.title1 = goo.titles[0].encode('utf-8')
                request.title2 = goo.titles[1].encode('utf-8')
                request.title3 = goo.titles[2].encode('utf-8')
                s.merge(request)
                s.commit()
                time.sleep(10)

def get_host_by_name():
    s = session()
    for request in s.query(Google_search_resaults).filter(Google_search_resaults.ip1 == None):
        if request.domain1 is not None:
            try:
                request.ip1 = gethostbyname(request.domain1).encode('utf-8')
                s.merge(request)
                logger.debug('get host by name %s : %s' % (request.domain1, request.ip1))
            except Exception as e:
                logger.critical('get host by name failed %s: %s %s' % (request.domain1, e.message, e.args))
    for request in s.query(Google_search_resaults).filter(Google_search_resaults.ip2 == None):
        if request.domain2 is not None:
            try:
                request.ip2 = gethostbyname(request.domain2).encode('utf-8')
                s.merge(request)
                logger.debug('get host by name %s : %s' % (request.domain2, request.ip2))
            except Exception as e:
                logger.critical('get host by name failed %s: %s %s' % (request.domain2, e.message, e.args))
    for request in s.query(Google_search_resaults).filter(Google_search_resaults.ip3 == None):
        if request.domain3 is not None:
            try:
                request.ip3 = gethostbyname(request.domain3).encode('utf-8')
                s.merge(request)
                logger.debug('get host by name %s : %s' % (request.domain3, request.ip3))
            except Exception as e:
                logger.critical('get host by name failed %s: %s %s' % (request.domain3, e.message, e.args))
    s.commit()

def get_country_by_ip():
    g = Grab()
    #g.setup(log_file='/scripts/log.html')
    g.setup(follow_location=True)
#    result = []
#    for it in ip:
    s = session()
    for request in s.query(Google_search_resaults).filter(Google_search_resaults.country1 == None):
        if request.ip1 is not None:
            try:
                g.go('http://www.ip2nation.com/ip2nation')
                g.doc.set_input("ip", request.ip1)
                g.doc.submit()
                request.country1 = g.doc.select('//div[@id="visitor-country"]').text().encode('utf-8')
                logger.debug('get country by ip %s : %s' % (request.ip1, request.country1))
            except Exception as e:
                logger.critical('get country by ip failed %s: %s %s' % (request.ip1, e.message, e.args))
            s.merge(request)
    for request in s.query(Google_search_resaults).filter(Google_search_resaults.country2 == None):
        if request.ip2 is not None:
            try:
                g.go('http://www.ip2nation.com/ip2nation')
                g.doc.set_input("ip", request.ip2)
                g.doc.submit()
                request.country2 = g.doc.select('//div[@id="visitor-country"]').text().encode('utf-8')
                logger.debug('get country by ip %s : %s' % (request.ip2, request.country2))
            except Exception as e:
                logger.critical('get country by ip failed %s: %s %s' % (request.ip2, e.message, e.args))
            s.merge(request)
    for request in s.query(Google_search_resaults).filter(Google_search_resaults.country3 == None):
        if request.ip3 is not None:
            try:
                g.go('http://www.ip2nation.com/ip2nation')
                g.doc.set_input("ip", request.ip3)
                g.doc.submit()
                request.country3 = g.doc.select('//div[@id="visitor-country"]').text().encode('utf-8')
                logger.debug('get country by ip %s : %s' % (request.ip3, request.country3))
            except Exception as e:
                logger.critical('get country by ip failed %s: %s %s' % (request.ip3, e.message, e.args))
            s.merge(request)
    s.commit()

def create_report():
    """
    Функция создает прайс лист в формате csv для покупателей, которые запрашивают прайс лист.
    csv файл для удобства можно сохранять как excel файл.
    """
    with open('/home/andrew/oboron_predrp.csv', 'wb') as f:
        wr = csv.writer(f, delimiter=';')
        wr.writerow([u'Ид'.encode('cp1251'), u'Предприятие'.encode('cp1251'),
                     u'Домен'.encode('cp1251'), u'Заголовок'.encode('cp1251'), u'IP адрес'.encode('cp1251'), u'Страна'.encode('cp1251'),
                     u'Домен2'.encode('cp1251'), u'Заголовок2'.encode('cp1251'), u'IP адрес2'.encode('cp1251'), u'Страна2'.encode('cp1251'),
                     u'Домен3'.encode('cp1251'), u'Заголовок3'.encode('cp1251'), u'IP адрес3'.encode('cp1251'), u'Страна3'.encode('cp1251')])
        s = session()
        def xstr(s):
            if s is None:
                return ''
            elif isinstance(s, str):
                try:
                    return s.decode('utf-8').encode('cp1251')
                except:
                    return 'wtf'
            elif isinstance(s, unicode):
                return s.encode('cp1251')

        for request in s.query(Google_search_resaults).all():
            row = ['' for x in range(14)]
            row[0] = xstr(request.id)
            row[1] = xstr(request.request)
            row[2] = xstr(request.domain1)
            row[3] = xstr(request.title1)
            row[4] = xstr(request.ip1)
            row[5] = xstr(request.country1)
            row[6] = xstr(request.domain2)
            row[7] = xstr(request.title2)
            row[8] = xstr(request.ip2)
            row[9] = xstr(request.country2)
            row[10] = xstr(request.domain3)
            row[11] = xstr(request.title3)
            row[12] = xstr(request.ip3)
            row[13] = xstr(request.country3)
            wr.writerow(row)

def get_contacts():

    s = session()
    p = Pool(1)
    domains = [x.domain1 for x in s.query(Google_search_resaults).all()]
    #for request in s.query(Google_search_resaults).all():
    #    domains.append(request.domain1)
    p.map(get_mail, domains)

def get_mail(domain):
    print domain
    s = session()
    contact = Contacts()
    contact.domain = domain.encode('utf-8')
    g = Grab(timeout=15)
    g.go('http://' + domain)
    try:
        #contacts_link = g.doc.select(u'//a[starts-with(.,"КОНТАКТЫ")]').attr('href')
        contact.contacts_link = g.doc.rex_search(u'<a.*?href="(.*?)">[Кк][Оо][Нн][Тт][Аа][Кк][Тт][ЫынН].*?</a>').group(1)
    except:
        logger.debug('web site %s does not have contact link' % domain)
        contact.email = 'None'
        contact.contacts_link = 'None'
        s.merge(contact)
        s.commit()
        return None
    if contact.contacts_link is not None:
        g.go(contact.contacts_link)
        try:
            contact.email = g.doc.rex_search('[\w\.-]+@[\w-]+\.[a-zA-Z]{2,4}').group(0)
            logger.info('%s Finded email: %s' % (domain, contact.email))
            if contact.email is None:
                logger.debug('%s email not found' % domain)
                contact.email = 'None'
                s.merge(contact)
                s.commit()
                return None
            else:
                contact.email = contact.email.encode('utf-8')
                s.merge(contact)
                s.commit()
                return contact.email
        except Exception as e:
            logger.critical('get email failed %s %s' % (e.message, e.args))
            return None


#get_mail('viam.ru')
get_contacts()
#goo = Google()
#print goo.search(u'Всероссийский научно-исследовательский институт авиационных материалов, г. Москва', count=3)
#print goo.domains
#read_reestr_from_excel()
#googling()
#get_host_by_name()
#get_country_by_ip()
#create_report()