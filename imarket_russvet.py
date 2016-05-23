#!/usr/bin/env python
# -*- coding: utf-8 -*-

SHORT_CATEGORY_LIST = '/scripts/web-scraping/short_category_list.xml'
IMG_DIR = '/home/andrew/images'
# TODO В базу записывается неправильный Replacement и не работает nextpage

import logging
import sys
import re
import os
from multiprocessing import Pool

from grab import Grab
import pycurl
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lxml import etree

class Category(object):
    def __init__(self, name):
        self.id = None
        self.name = name
        self.link = ''
        self.childrens = []

Base = declarative_base()
class Product(Base):

    __tablename__ = 'products'
    articul = Column(String(10), primary_key=True)
    name = Column(String(1000))
    # Ссылка на фото с сайта Russvet
    img_ref = Column(String(1000))
    # Расположение фото на нашем сервере
    my_img = Column(String(1000))
    category = Column(String(1000))
    wholesale_price = Column(Float)
    retail_price = Column(Float)
    description = Column(String(100000))
    unit = Column(String(10))
    # Категория товара относительно наличия на складе (складская, заказная)
    sklad = Column(String(10))
    available_quantity = Column(Integer)
    replacement = Column(String(1000))
    global_link = Column(String(1000))
    manufacturer = Column(String(1000))
    charact_downloaded = Column(Boolean)

    def __init__(self):
        self.articul = ''
        self.name = ''
        self.img_ref = ''
        self.my_img = ''
        self.category = ''
        self.wholesale_price = 0.0
        self.retail_price = 0.0
        self.description = ''
        self.unit = ''
        self.sklad = ''
        self.available_quantity = 0
        self.replacement = ''
        self.global_link = ''
        self.manufacturer = ''
        self.charact_downloaded = False

    def __repr__(self):
        return "<Goods('%s','%s', '%s', '%s', '%s')>" % \
               (self.articul, self.name,
                self.manufacturer, self.img_ref,
                self.category)

# Зададим параметры логгирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(u'%(filename)s[LINE:%(lineno)d]# '
                              u'%(levelname)-8s [%(asctime)s]  %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
# Подключение к базе данных
engine = create_engine('mysql://root:8-9271821473@localhost/russvet')
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)

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
    g.doc.submit()
    return g
def get_categories(g):
    """ Получаем все категории с сайта
    :param g: Grab
    """

    tree = etree.parse(SHORT_CATEGORY_LIST)
    # Парсим категории первого уровня
    category_level_1_selectors = g.doc.select('//td[@class="customCategory"]//a[@class="levelOne"]')
    categories = Category('top')
    for category_level_1_selector in category_level_1_selectors:
        # Проверяем, есть ли категория в шашем сокращенном списке категорий
        expr = u"//Группа[Наименование[text() = $name]]"
        xml_elem = tree.xpath(expr, name = category_level_1_selector.text())
        if xml_elem:
            category_level_1 = Category(category_level_1_selector.text())
            logger.debug('Level 1 %s' % category_level_1_selector.text())
            category_level_1.link = 'https://imarket.russvet.ru:5000/OA_HTML/' + \
                                    category_level_1_selector.attr('href')
            logger.debug(category_level_1_selector.attr('href'))
            categories.childrens.append(category_level_1)
            # Сохраняем ссылку на каталог в XML файл в атрибуте "Ссылка"
            link_elem = etree.SubElement(xml_elem[0], u"Ссылка")
            link_elem.text = category_level_1.link

    # Парсим категории второго уровня
    for category_level_1 in categories.childrens:
        g.go(category_level_1.link)
        for category_level_2_selector in g.doc.select('//td[@class="customCategory"]//a[@class="levelTwo"]'):
            category_level_2 = Category(category_level_2_selector.text())
            logger.debug('Level 2 %s' % category_level_2_selector.text())
            category_level_2.link = category_level_2_selector.attr('href')
            logger.debug(category_level_2_selector.attr('href'))
            category_level_1.childrens.append(category_level_2)
    with open(SHORT_CATEGORY_LIST, 'w') as f:
        f.write(etree.tostring(tree, pretty_print=True, encoding='utf-8'))
    return categories

def download_image(g, link):
    try:
        path = re.sub('http://catalog.russvet.ru', '', link)
        resp = g.go(link)
        if not os.path.exists(IMG_DIR + re.sub(r'[^/]*$','',path)):
            os.makedirs(IMG_DIR + re.sub(r'[^/]*$','',path))
        with open(IMG_DIR + path, 'w') as f:
            f.write(resp.body)
        my_link = u'http://кабель-13.рф' + path.decode('utf-8')
        return my_link
    except Exception as e:
        logger.warn('Error: %s %s' % (e.message, e.args))
        return ''

def grab_category(category):
    """ Парсим товары из категори второго уровня
    :type category: Category
    :type g: Grab
    :type s: SQL session
    """
    product = Product()
    # Подключение к базе данных
    engine = create_engine('mysql://root:8-9271821473@localhost/russvet')
    Session = sessionmaker(bind=engine)
    s = Session()
    # Настроим параметры Grab
    g = Grab()
    #g.transport.curl.setopt(pycurl.HEADER, True)
    g.transport.curl.setopt(pycurl.SSLCERT, '/scripts/web-scraping/rus-svet-cert.pem')
    g.transport.curl.setopt(pycurl.SSLCERTPASSWD, 'null95f353bd')
    g.cookies.load_from_file('/home/andrew/russvet_cookie')
    # Post запрос для отображения колонки брэндов
    try:
        g.go(category.link, post={'showPositionWithZero' : "false",
                                  'showPositionOnlyStore' : "false",
                                  'showBrand' : "true",
                                  'column' : "1",
                                  'radio' : "on",
                                  'sortAscOrDesc' : "false"})
    except:
        logger.warn('Can not open category page %s' % category.link)

    while True:
        for product_selector in g.doc.select('//table[@class="OraBGAccentDark"]//tr[starts-with(@class,"tab-row")]'):
            try:
                product.img_ref = product_selector.select('./td[2]/a').attr('id').encode('utf-8')
                logger.debug('Image: %s' % product.img_ref.decode('utf-8'))
            except:
                logger.debug('No image')
            try:
                product.my_img = download_image(g, product.img_ref).encode('utf-8')
                logger.debug('My image: %s' % product.my_img.decode('utf-8'))
            except:
                logger.warn('Can not download image %s' % product.img_ref)
            try:
                product.articul = product_selector.select('./td[3]').text().encode('utf-8')
                logger.debug('Articul: %s' % product.articul.decode('utf-8'))
            except:
                logger.debug('No articul')
            try:
                product.name = product_selector.select('./td[4]').text().encode('utf-8')
                logger.debug('Name: %s' % product.name.decode('utf-8'))
            except:
                logger.debug('No name')
            try:
                product.global_link = 'https://imarket.russvet.ru:5000/OA_HTML/' + \
                                      product_selector.select('./td[4]/a').attr('href').encode('utf-8')
                logger.debug('Link: %s' % product.global_link.decode('utf-8'))
            except:
                logger.debug('No product global link')
            try:
                product.sklad = product_selector.select('./td[5]').text().encode('utf-8')
                logger.debug('Sklad: %s' % product.sklad.decode('utf-8'))
            except:
                logger.debug('No sklad')
            try:
                product.available_quantity = product_selector.select('./td[6]').number()
                logger.debug('Available quantity: %s' % product.available_quantity)
            except:
                logger.debug('No availble quantity')
            try:
                product.unit = product_selector.select('./td[8]').text().encode('utf-8')
                logger.debug('Unit: %s' % product.unit.decode('utf-8'))
            except:
                logger.debug('No unit')
            try:
                product.wholesale_price = float(product_selector.select('./td[9]').text())
                logger.debug('Wholesale price: %s' % product.wholesale_price)
            except:
                logger.debug('No whosale price')
            try:
                product.replacement = 'https://imarket.russvet.ru:5000/OA_HTML/' + \
                                      product_selector.select('./td[11]/a').attr('href').encode('utf-8')
                logger.debug('Replacement: %s' % product.replacement.decode('utf-8'))
            except:
                logger.debug('No replacement')
            try:
                product.manufacturer = product_selector.select('./td[12]').text().encode('utf-8')
                logger.debug('Manufacturer: %s' % product.manufacturer.decode('utf-8'))
            except:
                logger.debug('No manufacturer')
                logger.debug('')
            s.merge(product)
            s.commit()
        if not g.doc.select(u'//td[@class="tableRecordNav"]/a[starts-with(.,"Следующ")]').exists():
            break
        else:
            try:
                next_link = g.doc.select(u'//td[@class="tableRecordNav"]/a[starts-with(.,"Следующ")]').attr('href')
                next_link = re.sub(r'^javascript.*?"', '', next_link)
                next_link = 'https://imarket.russvet.ru:5000/OA_HTML/' + next_link
                next_link = re.sub(r';.*?\?', '?', next_link)
                next_link = re.sub(r'".*$',
                                   '&showPositionWithZero=false&showPositionOnlyStore=false&showBrand=true&sortAscOrDesc=false&column=1',
                                   next_link)
                logger.debug('Next page: %s' % next_link)
                g.go(next_link)
            except:
                logger.warn('Can not go to the next page')
                break

def grab_all():
    logger.debug('Init...')
    g1 = init()
    g1.cookies.save_to_file('/home/andrew/russvet_cookie')
    tree = etree.parse(SHORT_CATEGORY_LIST)
    root = tree.getroot()
    short_category_list = root.iter(u'Наименование')
    print short_category_list
    root_category = get_categories(g1)
    for category_l1 in root_category.childrens:
        logger.info('Category l1: %s' % category_l1.name)
        if category_l1.name in short_category_list:
            for category_l2 in category_l1.childrens:
                logger.info('Category l2: %s' % category_l2.name)
                if category_l2.name in short_category_list:
                    grab_category(category_l2)
    #category_l = Category('name')
    #category_l.link = 'https://imarket.russvet.ru:5000/OA_HTML/ibeCCtpSctDspRte.jsp?section=11627&beginIndex=21&sitex=10082:52168:RU'
    #grab_category(category_l)

g = init()
get_categories(g)