#!/usr/bin/env python
# -*- coding: utf-8 -*-

from grab import Grab
import logging
import re
import sys
import os

from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Goods(Base):
    __tablename__ = 'goods'
    articul = Column(String(10), primary_key=True)
    name_from_site = Column(String(1000))
    name_from_price = Column(String(1000))
    img_ref = Column(String(1000))
    category = Column(String(1000))
    def __repr__(self):
        return "<Goods('%s','%s', '%s', '%s', '%s')>" % \
               (self.articul, self.name_from_site,
                self.name_from_price, self.img_ref,
                self.category)

class Category(object):
    def __init__(self, name):
        self.name = name
        self.link = ''
        self.children = []

class Svetservis(object):
    def __init__(self):
        self.g = Grab()
        self.categories = []
        # Зададим параметры логгирования
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(u'%(filename)s[LINE:%(lineno)d]# '
                                      u'%(levelname)-8s [%(asctime)s]  %(message)s')
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        # Подключение к базе данных
        self.engine = create_engine('mysql://root:8-9271821473@localhost/svetservis')
        self.session = sessionmaker()
        self.session.configure(bind=self.engine)
        Base.metadata.create_all(self.engine)
    def get_categories(self):
        try:
            self.g.go('http://store.svetservis.ru/map/')
        except:
            self.logger.warn('Can not access http://store.svetservis.ru/map/')
        ul_level_1_number = 1
        ul_level_1_selectors = self.g.doc.select('//div[@class="pod_cart"]/ul')
        for ul_level_1_selector in ul_level_1_selectors:
            category_level_1 = Category(ul_level_1_number)
            self.categories.append(category_level_1)
            ul_level_1_number += 1
            li_index = 1
            self.logger.debug(category_level_1.name)
            for category_level_2_selector in ul_level_1_selector.select('./li'):
                category_level_2 = Category(category_level_2_selector.text())
                category_level_1.children.append(category_level_2)
                self.logger.debug(category_level_2_selector.text())
                for category_level_3_selector in ul_level_1_selector.select('./ul[' + str(li_index) + ']/li'):
                    category_level_3 = Category(category_level_3_selector.text())
                    category_level_3.link = ul_level_1_selector.select('./ul[' + str(li_index) +
                                                                       ']/li/a').attr('href')
                    category_level_2.children.append(category_level_3)
                    self.logger.debug(category_level_3_selector.text())
                li_index += 1
    def download_image(self, img_ref):
        img_ref = 'http://store.svetservis.ru' + img_ref
        g = Grab()
        try:
            resp = g.go(img_ref)
        except:
            self.logger.warn('Can not access %s' % img_ref)
            return None
        # Создаем у себя такую же директорию, как на сайте, например:
        # img_ref = /UserFiles/Image/010104_provod_ustanovochnyj_mednyj/09000395_1s.jpg
        # img_directory ./UserFiles/Image/010104_provod_ustanovochnyj_mednyj/
        img_directory = '.' + re.sub(r'[^/]*$','',img_ref)
        if not os.path.exists(img_directory):
            os.makedirs(img_directory)
        with open('.' + img_ref, 'w') as img:
                img.write(resp.body)

    def get_goods(self):
        try:
            self.g.go('http://store.svetservis.ru/shop/CID_200027051.html')
        except:
            self.logger.warn('Can not access %s' %
                             'http://store.svetservis.ru/shop/CID_200027051.html')
            return None
        # Получаем название товара
        title = self.g.doc.select('//div[@class="IndexSpecMainDiv"]//a[@class="product_name"]').text()
        self.logger.debug('title: %s' % title)
        # Получаем карточку товара, которая представлена в виде таблицы
        good_table = self.g.doc.select(u'//div[@class="IndexSpecMainDiv"]//table//table[//a[@title="' +
                                       title + u'"]]')
        # Получаем артикул товара
        articul = good_table.select(u'//div[starts-with(.,"Артикул")]').text()
        articul = articul.split(' ')[-1]
        self.logger.debug(articul)
        # Получаем наименование товара как оно указано в прайсе из элемента "Характеристики"
        name_from_price = good_table.select('//div[@class="prodDesc"]').text()
        self.logger.debug(name_from_price)
        # Получаем изображение товара
        img_ref = self.g.doc.select(u"//div[@class='IndexSpecMainDiv']//img[contains(@title,'"
                                    + title + u"')]").attr('src')
        self.download_image(img_ref)
        # Сохраняем товар в базу данных
        goods = Goods(articul=articul.encode('utf-8'),
                      name_from_price=name_from_price.encode('utf-8'),
                      name_from_site=title.encode('utf-8'),
                      category='010102',img_ref=img_ref)
        s = self.session()
        s.merge(goods)
        s.commit()

        # Получаем ссылку на каталог товаров в виде прайс листа
        print self.g.doc.select(u'//div[@class="col-2"]//a[contains(.,"Прайс-лист каталога")]').attr('title')

    def get_goods_from_price_page(self, price_page_url):
        try:
            self.g.go(price_page_url)
        except:
            self.logger.warn('Can not access %s' % price_page_url)
            return None



s = Svetservis()
#s.get_categories()
s.get_goods()

"""
for category in s.categories:
    print '%s %s %s ' % (category.name, category.link, len(category.children))
    for it in category.children:
        print '%s %s %s ' % (it.name, it.link, len(it.children))
        for el in it.children:
            print '%s %s %s ' % (el.name, el.link, len(el.children))
"""