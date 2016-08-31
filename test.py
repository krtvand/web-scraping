#!/usr/bin/env python
# -*- coding: utf-8 -*-

MY_CATEGORY_LIST = '/home/andrew/my_cataloge.xml'
CABLE_CHARACTS = '/home/andrew/cable_characteristics.xml'
PRODUCT_CHARACTS = '/home/andrew/product_characteristics.xml'

import logging
import requests
import sys
import re
import os
from multiprocessing import Pool
from lxml import html
import csv
import xlwt

from grab import Grab
import pycurl
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lxml import etree
from itertools import izip_longest, izip
Base = declarative_base()
import time

from prestapyt import PrestaShopWebServiceError, PrestaShopWebService


class Product(Base):

    __tablename__ = 'products'
    articul = Column(String(10), primary_key=True)
    name = Column(String(1000))
    # Ссылка на фото с сайта Russvet
    img_ref = Column(String(1000))
    # Расположение фото на нашем сервере
    my_img = Column(String(1000))
    category_id = Column(String(10))
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
        self.wholesale_price = -1.0
        self.retail_price = -1.0
        self.description = ''
        self.unit = ''
        self.sklad = ''
        self.available_quantity = -1
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
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(u'%(filename)s[LINE:%(lineno)d]# '
                              u'%(levelname)-8s [%(asctime)s]  %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def login():
    g = Grab(timeout=25)
    #g.transport.curl.setopt(pycurl.HEADER, True)
    g.transport.curl.setopt(pycurl.SSLCERT, '/scripts/web-scraping/rus-svet-cert.pem')
    g.transport.curl.setopt(pycurl.SSLCERTPASSWD, 'null95f353bd')
    g.go('https://imarket.russvet.ru:5000')
    redirect_link = g.doc.select('//a').attr('href')
    logger.debug('redirect link %s' % redirect_link)
    try:
        g.go(redirect_link)
        g.doc.set_input_by_id('username', 'krtvand')
        g.doc.set_input_by_id('password', 'krtvand')
        g.doc.submit()
        g.cookies.save_to_file('/home/andrew/russvet_cookie')
    except Exception as e:
        logger.warn('Error when login: %s %s' % (e.message, e.args))
    return g

engine = create_engine('mysql://root:8-9271821473@localhost/russvet')


"""
# В базе должены быть товары из этой категории
category_l1_elems = tree.xpath(u"/Группы/Группа")
models = set()
for category_l2_id in category_l1_elems[0].xpath(u"Группы/Группа/Ид")[2:]:
    print category_l2_id.text
    for product in s.query(Product).filter(Product.category_id == category_l2_id.text).all():
        models.add(product.name.split()[1])
print len(models)
for m in models:
    etree.SubElement(cable_characts_tree.getroot(), "model").text = m.decode('utf-8')
with open('/home/andrew/cable_characteristics.xml', 'w') as f:
    f.write(etree.tostring(cable_characts_tree, pretty_print=True, encoding='utf-8'))

for ch in cable_characts_tree.findall('model'):
    q = ('%' + ch.text + '%').encode('utf-8')
    print ch.text, len(s.query(Product).filter(Product.name.like(q)).all())
#Note.query.filter(Note.message.like("%somestr%")).all()
"""


"""
url = 'http://xn---13-5cdfy6al7m.xn--p1ai/adminkrtvand/index.php?controller=AdminDashboard'
result = session_requests.get(
	url,
	headers = dict(referer = url)
)"""


"""
TODO в текущей версии проверка наличия характеристик проходит путем
проверки наличия товара с данным артикулом в XML файле,
но существуют товары, которые не имеют характеристик
(т.к. на сайте доноре их не было во время первого парсинга), но уже записаны в XML файл.
Таким образом, в системе появляются товары,
которые уже никогда не получат описание

#  Проходим по категориям, которые есть в сокращенном списке категорий,
# если проходить по всем товарам в базе данных,
# то в прайс включаются товары с устаревшими категориями,
# которые были добавлены ранее
category_tree = etree.parse(SHORT_CATEGORY_LIST)
# XML файл с характеристиками товаров
products_tree = etree.parse(PRODUCT_CHARACTS)
for category_id in category_tree.xpath(u"//Ид"):
    logger.info('Parsing features for %s' % category_id.text)
    for product in s.query(Product).filter(Product.category_id == category_id.text.encode('utf-8')).all():
    #for product in s.query(Product).filter(Product.category_id == '30163').all():
        # Проверяем, есть ли товар в шашем катологе характеристик товаров
        expr = "/products/product[articul[text() = $articul]]"
        xml_elem = products_tree.xpath(expr, articul=product.articul)
        # Если есть, выходим из функции, т.к. каждый товар
        # должен полностью описываться при первой записи в файл
        if len(xml_elem) > 0:
            continue
            # xml_elem[0].xpath('characteristics/model').text = 'test'
        else:
            logger.debug('Parsing features for %s', product.articul)
            try:
                g.go(product.global_link)
            except Exception as e:
                logger.warn('Can not open product page when parsing features for %s: %s %s' % (product.articul, e.message, e.args))
                with open(PRODUCT_CHARACTS, 'w') as f:
                    f.write(etree.tostring(products_tree, pretty_print=True, encoding='utf-8'))
                logger.info('Sleep for 4 minutes...')
                time.sleep(240)
                continue
            new_product = etree.SubElement(products_tree.getroot(), "product")
            etree.SubElement(new_product, "articul").text = product.articul
            etree.SubElement(new_product, "name").text = product.name.decode('utf-8')
            new_product_characts = etree.SubElement(new_product, "features")
            # Заполняем характеристики
            for ch_selector in g.doc.select('//table[@class="OraBGAccentDark"]//tr[@class="tableDataCell"]'):
                try:
                    etree.SubElement(new_product_characts, 'feature', {'name' : ch_selector.select('td[@class="tableDataCell"]')[0].text()}).text = ch_selector.select('td[@class="tableDataCell"]')[1].text()
                except Exception as e:
                    logger.warn('Error when parsing features for %s: %s %s' % (product.articul, e.message, e.args))
    with open(PRODUCT_CHARACTS, 'w') as f:
        f.write(etree.tostring(products_tree, pretty_print=True, encoding='utf-8'))
"""
"""
def delete_product(articul):
    # Удаляем характеристики товара
    products_tree = etree.parse('/home/andrew/test.xml')
    # Проверяем, есть ли товар в шашем катологе характеристик товаров
    expr = "/products/product[articul[text() = $articul]]"
    xml_elem = products_tree.xpath(expr, articul=articul)
    if len(xml_elem) > 0:
        xml_elem[0].getparent().remove(xml_elem[0])
        logger.info('Product %s is deleted from characteristics file' % articul)
    with open('/home/andrew/test.xml', 'w') as f:
        f.write(etree.tostring(products_tree, pretty_print=True, encoding='utf-8'))
    # Удаляем файл с изображением товара
    Session = sessionmaker(bind=engine)
    s = Session()
    product = s.query(Product).filter(Product.articul == articul.encode('utf-8')).first()
    if product is not None:
        try:
            os.remove(re.sub('http://localhost', '/var/www/html', product.my_img))
            logger.info('Deleted image of Product %s' % articul)
        except Exception as e:
            logger.warn('Error when deleting image %s: %s %s' % (articul, e.message, e.args))
    # Удаляем товар из базы
    try:
        s.delete(product)
        logger.info('Product %s is deleted from database file' % articul)
    except Exception as e:
        logger.warn('Error when deleting product %s from database: %s %s' % (articul, e.message, e.args))
"""
"""
    .*\s - пропускаем название
    (\d,?\d*)\*? - первая группа количество жил и символ звездочка(может отсутствовать),
    если в имени не указано количество жил, то в группу записывается сечение
    \s? - возможен пробел после звездочки
    (\d*,?\d*)? - вторая группа сечение (например 1,5) обязательная группа, может отсутствовать
    """

def create_price_excel():
    """
    Функция создает прайс лист в формате csv для покупателей, которые запрашивают прайс лист.
    csv файл для удобства можно сохранять как excel файл.
    """
    with open('/home/andrew/Electrosnab-opt-price.csv', 'wb') as f:
        wr = csv.writer(f, delimiter=';')
        wr.writerow([u'Артикул'.encode('cp1251'), u'Наименование'.encode('cp1251'),
                     u'Цена'.encode('cp1251'), u'Категория'.encode('cp1251'),
                     u'Производитель'.encode('cp1251'), u'Количество на складе'.encode('cp1251'),
                     u'Изображение'.encode('cp1251')])
        Session = sessionmaker(bind=engine)
        s = Session()
        #  Проходим по категориям, которые есть в сокращенном списке категорий,
        # если проходить по всем товарам в базе данных,
        # то в прайс включаются товары с устаревшими категориями,
        # которые были добавлены ранее
        tree = etree.parse(MY_CATEGORY_LIST)
        for category_id in tree.xpath(u"//Ид"):
            for product in s.query(Product).filter(Product.category_id == category_id.text.encode('utf-8')).order_by(
                    Product.name).all():
                row = ['' for x in range(7)]
                row[0] = product.articul
                # Делаем товар активным только в случае наличия его на складе
                if product.wholesale_price > 0 and product.available_quantity > 0:
                    row[1] = product.name.decode('utf-8').encode('cp1251')
                    # Получаем список категорий, к которым должен относиться товар.
                    # Если родительская категория относится к категории "Включает",
                    # то Ид этой категории мы пропускаем, и указываем только категории выше "Включает"
                    include_dir = category_id.xpath(u'ancestor::Включает')
                    if len(include_dir) == 0:
                        # В качестве категории указываем ее текущую директорию и все родительские
                        parents = [x.text for x in category_id.xpath(u"ancestor::*/Наименование")]
                    else:
                        # Иначе не указываем родителя
                        parents = [x.text for x in include_dir[0].xpath(u"ancestor::*/Наименование")]
                    row[3] = ', '.join(parents[::-1]).encode('cp1251')
                    # Проверяем, нет ли товара в списке акций (товары на главной)
                    expr = "/products/product[articul[text() = $articul]]"
                    row[2] = "%0.2f" % (product.wholesale_price * 1.05)
                    row[4] = product.manufacturer.decode('utf-8').encode('cp1251')
                    row[5] = product.available_quantity
                    # Image URLs (x,y,z...)
                    row[6] = re.sub('localhost', u'кабель-13.рф', product.my_img).encode('cp1251')
                    wr.writerow(row)

""" Сохранение в excel работает очень медленно
        style1 = xlwt.XFStyle()
        style1.num_format_str = '0.00'
        wb = xlwt.Workbook(encoding='cp1251')
        for category_l1 in tree.xpath(u"/Группы/Группа")[1:]:
            ws = wb.add_sheet(category_l1.find(u'Ид').text.encode('cp1251'))
            print category_l1.find(u'Ид').text
            row = 0
            for category_id in category_l1.xpath(u"//Ид"):
                for product in s.query(Product).filter(Product.category_id == category_id.text.encode('utf-8')).order_by(Product.name).all():
                    if product.wholesale_price > 0 and product.available_quantity > 0:
                        row += 1
                        ws.write(row, 0, product.articul)
                        ws.write(row, 1, product.name.decode('utf-8').encode('cp1251'))
                        # Получаем список категорий, к которым должен относиться товар.
                        # Если родительская категория относится к категории "Включает",
                        # то Ид этой категории мы пропускаем, и указываем только категории выше "Включает"
                        include_dir = category_id.xpath(u'ancestor::Включает')
                        if len(include_dir) == 0:
                            # В качестве категории указываем ее текущую директорию и все родительские
                            parents = [x.text for x in category_id.xpath(u"ancestor::*/Наименование")]
                        else:
                            # Иначе не указываем родителя
                            parents = [x.text for x in include_dir[0].xpath(u"ancestor::*/Наименование")]
                        ws.write(row, 3, ', '.join(parents[::-1]).encode('cp1251'))
                        # Проверяем, нет ли товара в списке акций (товары на главной)
                        ws.write(row, 2, product.wholesale_price * 1.05, style1)
                        ws.write(row, 4, product.manufacturer.decode('utf-8').encode('cp1251'))
                        ws.write(row, 5, product.available_quantity)
                        ws.write(row, 6, re.sub('localhost', u'кабель-13.рф', product.my_img).encode('cp1251'))
                wb.save('/home/andrew/example.xls')
"""

g = Grab()
s = set()
g.go('https://ru.bongacams.com')
for link in g.doc.select('//a[@class="chat"]'):
    s.add(link.attr('href'))

s2 = set()
g.setup(proxy='91.217.34.137:8080')
g.go('https://ru.bongacams.com')
for link in g.doc.select('//a[@class="chat"]'):
    s2.add(link.attr('href'))
with open('/home/andrew/bcams', 'w') as f:
    for link in s.difference(s2):
        f.write('https://ru.bongacams.com' + link)
        print 'https://ru.bongacams.com' + link


