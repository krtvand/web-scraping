#!/usr/bin/env python
# -*- coding: utf-8 -*-

SHORT_CATEGORY_LIST = '/scripts/web-scraping/short_category_list.xml'
IMG_DIR = '/home/andrew/images'
# TODO сохранять категорию для товара

import logging
import sys
import re
import os
from multiprocessing import Pool
from itertools import izip_longest
import time
import csv

from grab import Grab
import pycurl
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
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
        return "<Product('%s','%s', '%s', '%s', '%s')>" % \
               (self.articul, self.name,
                self.manufacturer, self.img_ref,
                self.category)

# Зададим параметры логгирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(u'%(filename)s[LINE:%(lineno)d]# '
                              u'%(levelname)-8s [%(asctime)s]  %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
# Подключение к базе данных
engine = create_engine('mysql://root:8-9271821473@localhost/russvet', poolclass=NullPool)
Base.metadata.create_all(engine)

# Авторизуемся на сайте
def login():
    g = Grab(timeout=25)
    #g.transport.curl.setopt(pycurl.HEADER, True)
    g.transport.curl.setopt(pycurl.SSLCERT, '/scripts/web-scraping/rus-svet-cert.pem')
    g.transport.curl.setopt(pycurl.SSLCERTPASSWD, 'null95f353bd')
    g.go('https://imarket.russvet.ru:5000')
    redirect_link = g.doc.select('//a').attr('href')
    logger.debug('redirect link %s' % redirect_link)
    g.go(redirect_link)
    g.doc.set_input_by_id('username', 'krtvand')
    g.doc.set_input_by_id('password', 'krtvand')
    g.doc.submit()
    g.cookies.save_to_file('/home/andrew/russvet_cookie')
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
            xml_link_elem = xml_elem[0].find(u"Ссылка")
            if xml_link_elem is not None:
                xml_link_elem.text = category_level_1.link
            else:
                xml_link_elem = etree.SubElement(xml_elem[0], u"Ссылка")
                xml_link_elem.text = category_level_1.link
    # Парсим категории второго уровня
    for category_level_1 in categories.childrens:
        g.go(category_level_1.link)
        for category_level_2_selector in g.doc.select('//td[@class="customCategory"]//a[@class="levelTwo"]'):
            # Проверяем, есть ли подкатегория в шашем сокращенном списке категорий
            expr = u"//Группа[Наименование[text() = $name]]"
            xml_elem = tree.xpath(expr, name = category_level_2_selector.text())
            if xml_elem:
                category_level_2 = Category(category_level_2_selector.text())
                category_level_2.link = 'https://imarket.russvet.ru:5000/OA_HTML/' + \
                                        category_level_2_selector.attr('href')
                category_level_2.link = re.sub('amp;', '', category_level_2.link)
                logger.debug(category_level_2.link)
                category_level_1.childrens.append(category_level_2)
                logger.debug('Level 2 %s' % category_level_2_selector.text())
                # Сохраняем ссылку на каталог в XML файл в атрибуте "Ссылка"
                xml_link_elem = xml_elem[0].find(u"Ссылка")
                if xml_link_elem is not None:
                    xml_link_elem.text = category_level_2.link
                else:
                    xml_link_elem = etree.SubElement(xml_elem[0], u"Ссылка")
                    xml_link_elem.text = category_level_2.link
    with open(SHORT_CATEGORY_LIST, 'w') as f:
        f.write(etree.tostring(tree, pretty_print=True, encoding='utf-8'))
    return categories

def download_image(g1, link):
    try:
        path = re.sub('http://catalog.russvet.ru', '', link)
        resp = g1.go(link)
        if not os.path.exists(IMG_DIR + re.sub(r'[^/]*$','',path)):
            os.makedirs(IMG_DIR + re.sub(r'[^/]*$','',path))
        with open(IMG_DIR + path, 'w') as f:
            f.write(resp.body)
        my_link = u'http://кабель-13.рф' + path.decode('utf-8')
        return my_link
    except Exception as e:
        logger.warn('Error when downloading image %s: %s %s' % (link.decode('utf-8'), e.message, e.args))
        return ''

def grab_category(category_link, category_name):
    """ Парсим товары из категори второго уровня
    :type category_link: Category_link
    :type category_name: category_name
    """
    engine.dispose()
    g = login()
    # Подключение к базе данных
    Session = sessionmaker(bind=engine)
    s = Session()
    # Настроим параметры Grab
    #g = Grab(timeout=25)
    #g.transport.curl.setopt(pycurl.HEADER, True)
    #g.transport.curl.setopt(pycurl.SSLCERT, '/scripts/web-scraping/rus-svet-cert.pem')
    #g.transport.curl.setopt(pycurl.SSLCERTPASSWD, 'null95f353bd')
    #g.cookies.load_from_file('/home/andrew/russvet_cookie')
    # Post запрос для отображения колонки брэндов
    try:
        g.go(category_link, post={'showPositionWithZero' : "false",
                                  'showPositionOnlyStore' : "false",
                                  'showBrand' : "true",
                                  'column' : "1",
                                  'radio' : "on",
                                  'sortAscOrDesc' : "false"})
    except Exception as e:
        logger.warn('Can not open category page %s: %s %s' % (category_link, e.message, e.args))
        return None
    number_of_products = 0
    while True:
        for product_selector in g.doc.select('//table[@class="OraBGAccentDark"]//tr[starts-with(@class,"tab-row")]'):
            try:
                articul = product_selector.select('./td[3]').text().encode('utf-8')
                logger.debug('Articul: %s' % articul.decode('utf-8'))
            except:
                logger.warning('No articul')
                continue
            product = s.query(Product).filter_by(articul=articul).first()
            if product is None:
                product = Product()
                product.articul = articul
            else:
                logger.debug('Product %s alredy in sql' % product.name.decode('utf-8'))
            if not product.img_ref:
                try:
                    product.img_ref = product_selector.select('./td[2]/a').attr('id').encode('utf-8')
                    logger.debug('Image: %s' % product.img_ref.decode('utf-8'))
                except:
                    logger.debug('No image')
            # Если имеется ссылка на изображение и но оно до сих пор не загружено, скачиваем его
            if not product.my_img and product.img_ref:
                try:
                    product.my_img = download_image(g.clone(), product.img_ref).encode('utf-8')
                    logger.debug('My image: %s' % product.my_img.decode('utf-8'))
                except:
                    logger.warn('Can not download image %s' % product.img_ref)
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
            product.category = category_name.encode('utf-8')
            logger.debug('')
            s.merge(product)
            s.commit()
            if product.available_quantity == 0:
                logger.info('Parsing %s category finished' % category_name)
                return
        number_of_products += 20
        logger.info('%s parsed %s products' % (category_name, number_of_products))
        # Переходим на следующую страницу товаров в катологе
        if not g.doc.select(u'//td[@class="tableRecordNav"]/a[starts-with(.,"Следующ")]').exists():
            logger.debug('Next link not exists')
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
        s.close()
        engine.dispose()
        return True


def create_csv(self):
    with open('/home/andrew/my_price.csv', 'wb') as f:
        wr = csv.writer(f, delimiter=';')
        wr.writerow(['ID',
                     'Active (0/1)', 'Name *', 'Categories (x,y,z...)',
                     'Price tax excluded or Price tax included',
                     'Tax rules ID', 'Wholesale price',
                     'On sale (0/1)', 'Discount amount',
                     'Discount percent', 'Discount from (yyyy-mm-dd)',
                     'Discount to (yyyy-mm-dd)', 'Reference #',
                     'Supplier reference #', 'Supplier',
                     'Manufacturer', 'EAN13',
                     'UPC', 'Ecotax', 'Width', 'Height',
                     'Depth', 'Weight', 'Quantity',
                     'Minimal quantity', 'Visibility',
                     'Additional shipping cost',
                     'Unity', 'Unit price', 'Short description',
                     'Description', 'Tags (x,y,z...)',
                     'Meta title', 'Meta keywords',
                     'Meta description', 'URL rewritten',
                     'Text when in stock', 'Text when backorder allowed',
                     'Available for order (0 = No, 1 = Yes)',
                     'Product available date', 'Product creation date',
                     'Show price (0 = No, 1 = Yes)', 'Image URLs (x,y,z...)',
                     'Delete existing images (0 = No, 1 = Yes)',
                     'Feature(Name:Value:Position)'])
        empty_row = ['' for x in range(46)]
        Session = sessionmaker(bind=engine)
        s = Session()
        for product in s.query(Product):
            if product.wholesale_price > 0 and product.articul:
                row = empty_row
                row[0] = product.articul.encode('utf-8')
                row[1] = '1'
                row[2] = product.name.encode('utf-8')
                row[3] = product.category.encode('utf-8')
                row[4] = product.wholesale_price * 1.2
                row[6] = product.wholesale_price
                row[12] = product.articul.encode('utf-8')
                row[13] = product.articul.encode('utf-8')
                row[23] = product.available_quantity
                row[38] = 1
                # Временно, при следующем парсинге убрать IP
                row[42] = product.my_img.encode('utf-8')
                wr.writerow(row)


def create_cataloge_csv(self):
    with open('/home/andrew/my_cataloge.csv', 'wb') as f:
        wr = csv.writer(f, delimiter=';')
        empty_row = ['' for x in range(11)]
        wr.writerow(empty_row)
        self.get_categories()
        for category in self.categories:
            row = empty_row
            row[0] = category.id
            row[2] = category.name.encode('utf-8')
            row[3] = 2
            row[4] = 0
            row[10] = 'http://194.54.64.90/UserFiles/categories/' + category.name.encode('utf-8') + '.jpg'
            wr.writerow(row)
            for it in category.children:
                empty_row = ['' for x in range(11)]
                row = empty_row
                row[0] = it.id
                row[2] = it.name.encode('utf-8')
                row[3] = category.id
                row[4] = 0
                wr.writerow(row)
                for el in it.children:
                    row = empty_row
                    row[0] = el.id
                    row[2] = el.name.encode('utf-8')
                    row[3] = it.id
                    row[4] = 0
                    wr.writerow(row)

def grab_category_helper(args):
    return grab_category(*args)

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

def grab_all():
    logger.debug('Init...')
    login()
    tree = etree.parse(SHORT_CATEGORY_LIST)
    root = tree.getroot()
    category_l1_elems = tree.xpath(u"/Группы/Группа")
    print len(category_l1_elems)
    p = Pool(10)
    for category_l1 in category_l1_elems:
        category_l1_name = category_l1.find(u'Наименование').text
        logger.info('Category l1: %s' % category_l1_name)
        print len(category_l1.xpath(u"Группы/Группа"))

        job_args = zip([x.text for x in category_l1.xpath(u"Группы/Группа/Ссылка")],
                       [x.text for x in category_l1.xpath(u"Группы/Группа/Наименование")])
        p.map(grab_category_helper, job_args)
        logger.info('Sleep for 5 minut...')
        time.sleep(300)
        """
        for chunk in grouper(job_args, 10):
            p = Pool(10)
            p.map(grab_category_helper, chunk)
            logger.info('Sleep for 1 minut...')
            time.sleep(60)
        """

        #for category_l2 in category_l1.xpath(u"Группы/Группа"):
        #    logger.info('Category l2: %s' % category_l2.find(u'Наименование').text)
        #    grab_category(category_l2.find(u'Ссылка').text)

grab_all()