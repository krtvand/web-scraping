#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys

from grab import Grab
import pycurl
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class Category(object):
    def __init__(self, name):
        self.id = None
        self.name = name
        self.link = ''
        self.children = []

Base = declarative_base()
class Product(Base):
    __tablename__ = 'products'
    articul = Column(String(10), primary_key=True)
    name = Column(String(1000))
    img_ref = Column(String(1000))
    category = Column(String(1000))
    wholesale_price = Column(Float)
    retail_price = Column(Float)
    description = Column(String(100000))
    unit = Column(String(10))
    sklad = Column(String(10))
    available_quantity = Column(Integer)
    replacement = Column(String(1000))
    global_link = Column(String(1000))
    manufacturer = Column(String(1000))

    def __repr__(self):
        return "<Goods('%s','%s', '%s', '%s', '%s')>" % \
               (self.articul, self.name_from_site,
                self.name_from_price, self.img_ref,
                self.category)

# Зададим параметры логгирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(u'%(filename)s[LINE:%(lineno)d]# '
                              u'%(levelname)-8s [%(asctime)s]  %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Авторизуемся на сайте
def init():
    g = Grab()
    g.transport.curl.setopt(pycurl.HEADER, True)
    g.transport.curl.setopt(pycurl.SSLCERT, '/scripts/web-scraping/rus-svet-cert.pem')
    g.transport.curl.setopt(pycurl.SSLCERTPASSWD, 'null95f353bd')
    g.go('https://imarket.russvet.ru:5000')
    redirect_link = g.doc.select('//a').attr('href')
    logger.debug('redirect link %s' % redirect_link)
    g.go(redirect_link)
    g.doc.set_input_by_id('username', 'krtvand')
    g.doc.set_input_by_id('password', 'krtvand')
    resp = g.doc.submit()
    return g
def get_categories(g):
    """ Получаем все категории с сайта
    :param g: Grab
    """
    # Парсим категории первого уровня
    category_level_1_selectors = g.doc.select('//td[@class="customCategory"]//a[@class="levelOne"]')
    categories = Category('top')
    for category_level_1_selector in category_level_1_selectors:
        category_level_1 = Category(category_level_1_selector.text())
        logger.debug('Level 1 %s' % category_level_1_selector.text())
        category_level_1.link = category_level_1_selector.attr('href')
        logger.debug(category_level_1_selector.attr('href'))
        categories.children.append(category_level_1)
    # Парсим категории второго уровня
    for category_level_1 in categories.children:
        g.go(category_level_1.link)
        for category_level_2_selector in g.doc.select('//td[@class="customCategory"]//a[@class="levelTwo"]'):
            category_level_2 = Category(category_level_2_selector.text())
            logger.debug('Level 2 %s' % category_level_2_selector.text())
            category_level_2.link = category_level_2_selector.attr('href')
            logger.debug(category_level_2_selector.attr('href'))
            category_level_1.children.append(category_level_2)

def grab_category(g, category):
    """ Парсим товары из категори второго уровня
    :type category: Category
    :type g: Grab
    """
    product = Product()
    g.go(category.link)
    print g.doc.select('//input[@id="sbr"]').exists()
    g.doc.set_input_by_xpath('//input[@id="sbr"]', True)
    resp = g.doc.submit(submit_name=u'Обновить')
    with open('/home/andrew/test.html', 'w') as f:
        f.write(resp.body)
    for product_selector in g.doc.select('//table[@class="OraBGAccentDark"]//tr[starts-with(@class,"tab-row")]'):
        product.articul = product_selector.select('./td[3]').text()
        logger.debug('Articul: %s' % product.articul)
        product.name = product_selector.select('./td[4]').text()
        logger.debug('Name: %s' % product.name)
        product.global_link = 'https://imarket.russvet.ru:5000/OA_HTML/' + product_selector.select('./td[4]/a').attr('href')
        logger.debug('Link: %s' % product.global_link)
        product.sklad = product_selector.select('./td[5]').text()
        logger.debug('Sklad: %s' % product.sklad)
        product.available_quantity = product_selector.select('./td[6]').number()
        logger.debug('Available quantity: %s' % product.available_quantity)
        product.unit = product_selector.select('./td[8]').text()
        logger.debug('Unit: %s' % product.unit)
        product.wholesale_price = float(product_selector.select('./td[9]').text())
        logger.debug('Wholesale price: %s' % product.wholesale_price)
        product.replacement = 'https://imarket.russvet.ru:5000/OA_HTML/' + product_selector.select('./td[11]/a').attr('href')
        logger.debug('Replacement: %s' % product.replacement)
        product.manufacturer = product_selector.select('./td[12]').text()
        logger.debug('Manufacturer: %s' % product.manufacturer)
        logger.debug('')

logger.debug('Init...')
g_l = init()
category_l = Category('name')
category_l.link = 'https://imarket.russvet.ru:5000/OA_HTML/ibeCCtpSctDspRte.jsp?section=12245&sitex=10082:52168:RU'
grab_category(g_l, category_l)