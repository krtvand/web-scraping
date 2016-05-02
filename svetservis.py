#!/usr/bin/env python
# -*- coding: utf-8 -*-

from grab import Grab
import logging
import re
import sys
import os

from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
#from tidylib import tidy_document

Base = declarative_base()

class Goods(Base):
    __tablename__ = 'goods'
    articul = Column(String(10), primary_key=True)
    name_from_site = Column(String(1000))
    name_from_price = Column(String(1000))
    img_ref = Column(String(1000))
    category = Column(String(1000))
    price = Column(Float)
    description = Column(String(100000))
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
                    category_level_3.link = 'http://store.svetservis.ru' + \
                                            ul_level_1_selector.select('./ul[' + str(li_index) +
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

    def get_goods_from_price_page(self, price_page_url):
        try:
            self.g.go(price_page_url)
        except:
            self.logger.warn('Can not access %s' % price_page_url)
            return None

    def get_goods(self, card_selector):
        # Получаем наименование товара
        try:
            title = card_selector.select('./tr/td/table/tr/div/a[@class="product_name"]').text()
            self.logger.debug('Title: %s' % title)
        except:
            self.logger.warning('Data not found for "title"')
            title = ''
        # Получаем артикул товара
        try:
            articul = card_selector.select(u'./tr/td/div[starts-with(.,"Артикул")]').text()
            articul = articul.split(' ')[-1]
            self.logger.debug('Articul: %s' % articul)
        except:
            self.logger.critical('Data not found for "articul"')
            return None
        # Получаем наименование товара как оно указано в прайсе из элемента "Характеристики"
        try:
            name_from_price = card_selector.select('./tr/td/div[@class="prodDesc"]').text()
            self.logger.debug('Name from price: %s' % name_from_price)
        except:
            self.logger.warning('Data not found for "name_from_price"')
            name_from_price = ''
        # Получаем изображение товара
        try:
            img_ref = card_selector.select("./tr/td/table/tr/td/a/img").attr('src')
            self.download_image(img_ref)
            self.logger.debug('Image reference: %s' % img_ref)
        except:
            self.logger.warning('Problems with image downloading')
            img_ref = ''
        # Сохраняем товар в базу данных
        goods = Goods(articul=articul.encode('utf-8'),
                      name_from_price=name_from_price.encode('utf-8'),
                      name_from_site=title.encode('utf-8'), img_ref=img_ref.encode('utf-8'))
        return goods
        # Получаем ссылку на каталог товаров в виде прайс листа
        #print self.g.doc.select(u'//div[@class="col-2"]//a[contains(.,"Прайс-лист каталога")]').attr('title')

    def scrap_category(self, category):
        g = Grab()
        try:
            g.go(category.link)
        except:
            self.logger.warn('Can not access %s' % category.link)
            return None
        # Переходим по ссылке "Все позиции", если в категории меньше 10 позиций,
        #  то ссылка отсутствует, поэтому используем изначальную ссылку на категорию
        try:
            category_all_ref = g.doc.select(u'//a[@title="Все позиции"]').attr('href')
        except:
            self.logger.debug('Data not found for link "All goods"')
            category_all_ref = category.link
        try:
            g.go(category_all_ref)
        except:
            self.logger.warn('Can not access %s' % category.link)
            return None
        try:
            cards = g.doc.select('//div[@class="IndexSpecMainDiv"]/table/tr/td/table')
        except:
            self.logger.critical('Can not get cards for goods in category %s' % category.link)
            return None
        # Обходим все товары в категории и сразу сохраняем их в БД
        for card in cards:
            goods = self.get_goods(card)
            if goods is not None:
                goods.category = category.name.encode('utf-8')
                s = self.session()
                s.merge(goods)
                s.commit()

    def scrap_all(self):
        self.get_categories()
        for category in self.categories:
            for it in category.children:
                for el in it.children:
                    #self.scrap_category(el)
                    print '%s %s %s ' % (el.name, el.link, len(el.children))


s = Svetservis()
#s.get_categories()
#s.scrap_category('http://store.svetservis.ru/shop/CID_200027051.html')
s.scrap_all()

"""
for category in s.categories:
    print '%s %s %s ' % (category.name, category.link, len(category.children))
    for it in category.children:
        print '%s %s %s ' % (it.name, it.link, len(it.children))
        for el in it.children:
            print '%s %s %s ' % (el.name, el.link, len(el.children))
"""