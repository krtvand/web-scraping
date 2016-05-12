#!/usr/bin/env python
# -*- coding: utf-8 -*-

from grab import Grab
import logging
import re
import sys
import os
import csv

import xlrd
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
    wholesale_price = Column(Float)
    description = Column(String(100000))
    unit = Column(String(10))

    def __repr__(self):
        return "<Goods('%s','%s', '%s', '%s', '%s')>" % \
               (self.articul, self.name_from_site,
                self.name_from_price, self.img_ref,
                self.category)

class Category(object):
    def __init__(self, name):
        self.id = None
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

    def read_price(self):
        try:
            rb = xlrd.open_workbook('/home/andrew/price.xls')
        except:
            self.logger.critical('Can not open price file!')
            return
        goods = Goods()
        sheet = rb.sheet_by_index(0)
        for row_index in range(sheet.nrows):
            # Сохраняем артикул в нужном формате, и пропускаем строку,
            # если нет записи для артикула
            articul = sheet.row(row_index)[1].value
            if isinstance(articul, float):
                goods.articul = ('%07.0f' % articul)
            else:
                continue
            name = sheet.row(row_index)[2].value
            # Удаляем пробельные символы в начале и в кноце строки
            name = name.lstrip().rstrip()
            goods.name_from_price = name.encode('utf-8')
            price = sheet.row(row_index)[3].value
            goods.wholesale_price = price
            unit = sheet.row(row_index)[4].value.lstrip().rstrip()
            goods.unit = unit.encode('utf-8')
            s = self.session()
            s.merge(goods)
            s.commit()

    def get_categories(self):
        """ Получаем все категории товаров
        """
        try:
            self.g.go('http://store.svetservis.ru/map/')
        except:
            self.logger.warn('Can not access http://store.svetservis.ru/map/')
        # Используем в качестве id категорий инкрементный индекс начиная с 10ти
        category_id = 10
        ul_level_1_selectors = self.g.doc.select('//div[@class="pod_cart"]/ul')
        category_level_1_names = self.g.doc.select('//div[@class="pod_cart"]/p/b')

        for ul_level_1_selector, category_level_1_name in zip(ul_level_1_selectors, category_level_1_names):
            category_level_1 = Category(category_level_1_name.text())
            category_level_1.id = category_id
            category_id += 1
            self.categories.append(category_level_1)
            li_index = 1
            self.logger.debug('%s (id=%s)' % (category_level_1.name, category_level_1.id))
            for category_level_2_selector in ul_level_1_selector.select('./li'):
                category_level_2 = Category(category_level_2_selector.text())
                category_level_2.id = category_id
                category_id += 1
                category_level_1.children.append(category_level_2)
                self.logger.debug('%s (id=%s)' % (category_level_2.name, category_level_2.id))
                for category_level_3_selector in ul_level_1_selector.select('./ul[' + str(li_index) + ']/li'):
                    category_level_3 = Category(category_level_3_selector.text())
                    category_level_3.id = category_id
                    category_id += 1
                    category_level_3.link = 'http://store.svetservis.ru' + category_level_3_selector.select('./a').attr('href')
                    category_level_2.children.append(category_level_3)
                    self.logger.debug('%s (id=%s)' % (category_level_3.name, category_level_3.id))
                li_index += 1

    def download_image(self, img_ref):
        """Загружаем изображение товара в такую же папку, как она лежала на сервере
        :param img_ref: ссылка на изображение
        :return:
        """
        #img_ref = 'http://store.svetservis.ru' + img_ref
        g = Grab()
        try:
            resp = g.go('http://store.svetservis.ru' + img_ref)
        except:
            self.logger.warn('Can not access %s' % img_ref)
            return None
        # Создаем у себя такую же директорию, как на сайте, например:
        # img_ref = /UserFiles/Image/010104_provod_ustanovochnyj_mednyj/09000395_1s.jpg
        # img_directory /var/www/html/UserFiles/Image/010104_provod_ustanovochnyj_mednyj/
        img_directory = '/var/www/html' + re.sub(r'[^/]*$','',img_ref)
        if not os.path.exists(img_directory):
            os.makedirs(img_directory)
        with open('/var/www/html' + img_ref, 'w') as img:
                img.write(resp.body)

    def get_goods_from_price_page(self, price_page_url):
        try:
            self.g.go(price_page_url)
        except:
            self.logger.warn('Can not access %s' % price_page_url)
            return None

    def get_goods(self, card_selector):
        """Парсим одну товарную позицию
        :param card_selector: селектор таблицы, в которой описан товар
        :return: экземпляр класса Товар (Goods)
        """
        # Получаем наименование товара
        try:
            title = card_selector.select('./tr/td/table/tr/div/a[@class="product_name"]').text().lstrip().rstrip()
            self.logger.debug('Title: %s' % title)
        except:
            self.logger.warning('Data not found for "title"')
            title = ''
        # Получаем артикул товара
        try:
            articul = card_selector.select(u'./tr/td/div[starts-with(.,"Артикул")]').text().lstrip().rstrip()
            articul = articul.split(' ')[-1]
            self.logger.debug('Articul: %s' % articul)
        except:
            self.logger.critical('Data not found for "articul"')
            return None
        # Получаем наименование товара как оно указано в прайсе из элемента "Характеристики"
        try:
            name_from_price = card_selector.select('./tr/td/div[@class="prodDesc"]').text().lstrip().rstrip()
            self.logger.debug('Name from price: %s' % name_from_price)
        except:
            self.logger.warning('Data not found for "name_from_price"')
            name_from_price = ''
        # Получаем изображение товара
        try:
            img_ref = card_selector.select("./tr/td/table/tr/td/a/img").attr('src')
            self.download_image(img_ref)
            self.logger.debug('Image reference: %s' % img_ref)
            full_img_ref = '194.54.64.90' + img_ref
        except:
            self.logger.warning('Problems with image downloading')
            img_ref = ''
        # Сохраняем товар в базу данных
        goods = Goods(articul=articul.encode('utf-8'),
                      name_from_price=name_from_price.encode('utf-8'),
                      name_from_site=title.encode('utf-8'), img_ref=full_img_ref.encode('utf-8'))
        return goods
        # Получаем ссылку на каталог товаров в виде прайс листа
        #print self.g.doc.select(u'//div[@class="col-2"]//a[contains(.,"Прайс-лист каталога")]').attr('title')

    def scrap_category(self, category):
        """ Парсим одну категорию 3 уровня
        :param category: экземпляр класса "Категория"
        :return: В случае ошибки возвращает None, в случае успеха ничего не возвращает
        """
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
        """ Парсим весь сайт, проходя через каждую категорию 3 уровня
        """
        self.get_categories()
        for category in self.categories:
            for it in category.children:
                for el in it.children:
                    self.scrap_category(el)

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
                         'Manufacturer','EAN13',
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
            session = self.session()
            for goods in session.query(Goods):
                if goods.wholesale_price and goods.articul and \
                        goods.name_from_price and goods.category:
                    row = empty_row
                    row[0] = goods.articul.encode('utf-8')
                    row[1] = '1'
                    row[2] = goods.name_from_price
                    row[3] = goods.category
                    row[4] = goods.wholesale_price * 1.2
                    row[6] = goods.wholesale_price
                    row[12] = goods.articul
                    row[13] = goods.articul
                    row[23] = 10
                    row[38] = 1
                    # Временно, при следующем парсинге убрать IP
                    row[42] = 'http://194.54.64.90' + goods.img_ref
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
s = Svetservis()
#s.scrap_all()
#s.read_price()
s.create_cataloge_csv()