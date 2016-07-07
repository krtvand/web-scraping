#!/usr/bin/env python
# -*- coding: utf-8 -*-

# TODO При заполнении марки кабеля проверять не наличие ID товара в XML, а именно марки

SHORT_CATEGORY_LIST = '/home/andrew/my_cataloge.xml'
IMG_DIR = '/var/www/html/images'
CABLE_CHARACTS = '/home/andrew/cable_characteristics.xml'
PRODUCT_CHARACTS = '/home/andrew/product_characteristics.xml'
HOME_FEATURED_PRODUCTS = '/home/andrew/home_featured_products.xml'

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
    try:
        g.go(redirect_link)
        g.doc.set_input_by_id('username', 'krtvand')
        g.doc.set_input_by_id('password', 'krtvand')
        g.doc.submit()
        g.cookies.save_to_file('/home/andrew/russvet_cookie')
    except Exception as e:
        logger.warn('Error when login: %s %s' % (e.message, e.args))
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
        resp = g1.go(link)
        path = re.sub('http://catalog.russvet.ru', '', link)
        path = re.sub('http://www.elektro-online.de', '', path)
        if not os.path.exists(IMG_DIR + re.sub(r'[^/]*$','',path)):
            os.makedirs(IMG_DIR + re.sub(r'[^/]*$','',path))
        with open(IMG_DIR + path, 'w') as f:
            f.write(resp.body)
        my_link = u'http://localhost/images' + path.decode('utf-8')
        return my_link
    except Exception as e:
        logger.warn('Error when downloading image %s: %s %s' % (link.decode('utf-8'), e.message, e.args))
        return ''

def grab_category(category_link, category_id):
    """ Парсим товары из категори второго уровня
    :type category_link: Category_link
    :type category_name: category_name
    """
    engine.dispose()
    try:
        g = login()
    except:
        logger.warning('Can not login! %s %s' % (category_link, category_id))
        return False
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
        resp = g.go(category_link, post={'showPositionWithZero' : "false",
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
        # Иногда появляется ошибка при парсинге категории
        try:
            g.doc.select('//table[@class="OraBGAccentDark"]//tr[starts-with(@class,"tab-row")]')
        except:
            with open('/home/andrew/error.html', 'w') as f:
                f.write(resp.body)
            logger.warning('Can not find category table! %s %s' % (category_link, category_id))
            return False
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
            # Каждому товару присваиваем имя категории и родительской котегории через спец разделитель "$"
            product.category_id = category_id
            logger.debug('Category id: %s' % product.category_id.decode('utf-8'))
            logger.debug('')
            s.merge(product)
            if product.available_quantity == 0:
                logger.info('Parsing %s category finished' % category_id)
                s.commit()
                s.close()
                engine.dispose()
                return True
        number_of_products += 20
        logger.info('%s parsed %s products' % (category_id, number_of_products))
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
        s.commit()
        s.close()
        engine.dispose()
    return True

def create_price():
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
        Session = sessionmaker(bind=engine)
        s = Session()
        #  Проходим по категориям, которые есть в сокращенном списке категорий,
        # если проходить по всем товарам в базе данных,
        # то в прайс включаются товары с устаревшими категориями,
        # которые были добавлены ранее
        tree = etree.parse(SHORT_CATEGORY_LIST)
        characts_tree = etree.parse(PRODUCT_CHARACTS)
        home_featured_products_tree = etree.parse(HOME_FEATURED_PRODUCTS)
        for category_id in tree.xpath(u"//Ид"):
            for product in s.query(Product).filter(Product.category_id == category_id.text.encode('utf-8')).all():
                row = ['' for x in range(45)]
                row[0] = product.articul
                # Делаем товар активным только в случае наличия его на складе
                if product.wholesale_price > 0 and product.available_quantity > 0:
                    row[1] = '1'
                else:
                    row[1] = '0'
                # Prestashop допускает максимальную длину наименования
                # не более 128 символов
                if len(product.name) > 128:
                    product.name = product.name.decode('utf-8')
                    product.name = product.name[0:127]
                    product.name = product.name.encode('utf-8')
                # Исключаем/меняем недопустимые символы в названии товара
                product.name = re.sub('=', '-', product.name)
                product.name = re.sub(';', ',', product.name)
                row[2] = product.name
                # В качестве категории указываем ее текущую директорию и все родительские
                parents = [x.text for x in category_id.xpath(u"ancestor::*/Ид")]
                row[3] = ', '.join(parents)
                # Проверяем, нет ли товара в списке акций (товары на главной)
                expr = "/products/product[articul[text() = $articul]]"
                xml_elem = home_featured_products_tree.xpath(expr, articul=product.articul)
                if len(xml_elem) > 0:
                    # Указываем специальную цену
                    row[4] = product.wholesale_price * float(xml_elem[0].xpath('markup')[0].text)
                    # Добавляем категорию "home" (id = 2)
                    row[3] = ', '.join([row[3], '2'])
                # Иначе присваиваем стандартную наценку 20%
                else:
                    row[4] = product.wholesale_price * 1.2
                row[6] = product.wholesale_price
                row[12] = product.articul
                row[13] = product.articul
                row[15] = product.manufacturer
                row[23] = product.available_quantity
                row[38] = 1
                # Image URLs (x,y,z...)
                row[42] = product.my_img
                # Delete existing images (0 = No, 1 = Yes)
                row[43] = 1
                # Проверяем, есть ли характеристики товара в нашем катологе товаров
                expr = "/products/product[articul[text() = $articul]]"
                xml_elem = characts_tree.xpath(expr, articul=product.articul)
                if len(xml_elem) > 0:
                    try:
                        # Из XML файла берем характеристики и
                        # приводим к следующему формату:
                        # Характеристика1:значение1, характеристика2:значение2 и т.д.
                        # при этом имя характеристики и ее значение
                        # не может содержать такие символы как ^<>;=#{}
                        features = ', '.join([':'.join([re.sub(r'[\^<>;=#{}]+', ' ', x.get('name')),
                                                        re.sub(r'[\^<>;=#{}]+', ' ', x.text)])
                                              for x in xml_elem[0].xpath('features/*')
                                              if x.text is not None])
                        row[44] = features.encode('utf-8')
                    except Exception as e:
                        logger.warn('Error when reading features for %s: %s %s' % (product.articul, e.message, e.args))
                    #row[44] = xml_elem[0].xpath('characteristics/model')[0].tag
                wr.writerow(row)

def create_cataloge_csv():
    tree = etree.parse(SHORT_CATEGORY_LIST)
    with open('/home/andrew/my_cataloge.csv', 'wb') as f:
        wr = csv.writer(f, delimiter=';')
        empty_row = ['' for x in range(11)]
        wr.writerow(empty_row)
        for category_l1 in tree.xpath(u"/Группы/Группа"):
            row = ['' for x in range(11)]
            row[0] = category_l1.find(u'Ид').text.encode('utf-8')
            if category_l1.find(u'Алиас') is not None:
                category_l1_name = category_l1.find(u'Алиас').text
            else:
                category_l1_name = category_l1.find(u'Наименование').text
            # Исключаем из названия запрещенные символы, которые не пропускает престашоп
            # также меняем слэш на символ '|', т.к. слэш распознается как вложенная категория
            if re.search(r'[;&]', category_l1_name):
                category_l1_name = re.sub(r'[;&]', ' ', category_l1_name)
            category_l1_name = re.sub('/', '|', category_l1_name)
            row[2] = category_l1_name.encode('utf-8')
            row[3] = 2
            row[4] = 0
            #row[10] = 'http://194.54.64.90/UserFiles/categories/' + category.name.encode('utf-8') + '.jpg'
            wr.writerow(row)
            for category_l2 in category_l1.xpath(u"Группы/Группа"):
                row = ['' for x in range(11)]
                category_id = category_l2.find(u'Ид').text.encode('utf-8')
                row[0] = category_id
                if category_l2.find(u'Алиас') is not None:
                    name = category_l2.find(u'Алиас').text
                else:
                    name = category_l2.find(u'Наименование').text
                # Пустые категории делаем неактивными
                Session = sessionmaker(bind=engine)
                s = Session()
                # В базе должены быть товары из этой категории
                if s.query(Product).filter(Product.category_id == category_id).filter(Product.available_quantity > 0).all():
                    row[1] = 1
                # Либо это категория должна быть родительской
                elif len(category_l2.xpath(u"Группы/Группа")) > 0:
                    row[1] = 1
                else:
                    row[1] = 0
                    logger.info('Category %s is empty' % name)
                # Исключаем из названия запрещенные символы, которые не пропускает престашоп
                if re.search(r'[;&]', name):
                    name = re.sub(r'[;&]', ' ', name)
                row[2] = re.sub('/', '|', name).encode('utf-8')
                row[3] = category_l1.find(u'Ид').text.encode('utf-8')
                row[4] = 0
                wr.writerow(row)
                for category_l3 in category_l2.xpath(u"Группы/Группа"):
                    row = ['' for x in range(11)]
                    category_id = category_l3.find(u'Ид').text.encode('utf-8')
                    row[0] = category_id
                    if category_l3.find(u'Алиас') is not None:
                        name = category_l3.find(u'Алиас').text
                    else:
                        name = category_l3.find(u'Наименование').text
                    # Пустые категории делаем неактивными
                    if s.query(Product).filter(Product.category_id == category_id).filter(
                                    Product.available_quantity > 0).all():
                        row[1] = 1
                    else:
                        row[1] = 0
                        logger.info('Category %s is empty' % name)
                    # Исключаем из названия запрещенные символы, которые не пропускает престашоп
                    if re.search(r'[;&]', name):
                        name = re.sub(r'[;&]', ' ', name)
                    row[2] = re.sub('/', '|', name).encode('utf-8')
                    row[3] = category_l2.find(u'Ид').text.encode('utf-8')
                    row[4] = 0
                    wr.writerow(row)


def grab_category_helper(args):
    return grab_category(*args)

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

def select_cable_characts(product):
    """Функция добавляет еще одну характеристику "Марка"
    к имеющимся в XML файл с характеристиками товаров
    для категорий "Кабели и провода".
    :param product:
    :return:
    """
    cable_characts_tree = etree.parse(CABLE_CHARACTS)
    # Считываем список возможных марок(моделей) кабеля
    characts_set = {ch.text for ch in cable_characts_tree.iter('model')}
    products_tree = etree.parse(PRODUCT_CHARACTS)
    # Проверяем, есть ли товар в шашем катологе товаров
    expr = "/products/product[articul[text() = $articul]]"
    xml_elem = products_tree.xpath(expr, articul=product.articul)
    # Если есть, выходим из функции, т.к. каждый товар
    # должен полностью описываться при первой записи в файл
    if len(xml_elem) > 0:
        if xml_elem[0].xpath(u'features/feature[@name = "Марка"]'):
            return True
        else:
            # Определяем марку кабеля как пересечение возможных
            # типов кабеля с каждым словом из названия продукта
            split_name = set(re.split('[\s/]', product.name.decode('utf-8')))
            try:
                etree.SubElement(xml_elem[0].xpath('features')[0], 'feature', {'name' : u'Марка'}).text = (characts_set & split_name).pop()
                with open(PRODUCT_CHARACTS, 'w') as f:
                    f.write(etree.tostring(products_tree, pretty_print=True, encoding='utf-8'))
            except:
                # В случае, если в наименовании товара отсутствует марка кабеля из списка,
                # мы получаем ошибку 'pop from an empty set'. Данную ошибку пропускаем.
                pass
        #xml_elem[0].xpath('characteristics/model').text = 'test'
    else:
        logger.warning('Product %s exists in database but not in XML file with characteristics' % product.articul)
        return False
    """
    else:
        new_product = etree.SubElement(products_tree.getroot(), "product")
        etree.SubElement(new_product, "articul").text = product.articul
        etree.SubElement(new_product, "name").text = product.name.decode('utf-8')
        new_product_characts = etree.SubElement(new_product, "characteristics")
        # Определяем марку кабеля как пересечение возможных
        # типов кабеля с каждым словом из названия продукта
        split_name = set(re.split('[\s/]', product.name.decode('utf-8')))
        try:
            etree.SubElement(new_product_characts, u'Марка').text = (characts_set & split_name).pop()
        except:
            pass
            #else:
        #    logger.warning('Can not find model for cable %s' % product.name)

    with open(PRODUCT_CHARACTS, 'w') as f:
        f.write(etree.tostring(products_tree, pretty_print=True, encoding='utf-8'))
    """

def grab_cable_characts():
    """ Получаем марку кабеля для все товаров из категории "Кабели и провода"
    """
    Session = sessionmaker(bind=engine)
    s = Session()
    tree = etree.parse(SHORT_CATEGORY_LIST)
    # Получаем все id подкатегорий Кабели и провода (нулевой в списке категорий)
    for category_id in tree.xpath(u"/Группы/Группа")[0].xpath(u"Группы/Группа/Ид"):
        for product in s.query(Product).filter(Product.category_id == category_id.text).all():
            select_cable_characts(product)

def grab_all():
    logger.debug('Init...')
    login()
    tree = etree.parse(SHORT_CATEGORY_LIST)
    root = tree.getroot()
    category_l1_elems = tree.xpath(u"/Группы/Группа")

    p = Pool(10)
    for category_l1 in category_l1_elems:
        category_l1_name = category_l1.find(u'Наименование').text
        logger.info('(%s/%s) Category l1: %s ' % (category_l1_elems.index(category_l1),
                                                  len(category_l1_elems), category_l1_name))
        job_args = zip([x.text for x in category_l1.xpath(u"Группы/Группа/Ссылка")],
                       [x.text for x in category_l1.xpath(u"Группы/Группа/Ид")])
        p.map(grab_category_helper, job_args)
        logger.info('Sleep for 4 minutes...')
        time.sleep(240)
        # Если категория второго уровня имеет подкатегории, продолжаем парсинг
        if category_l1.findall(u'Группы/Группа/Группы/Ид'):
            for category_l2 in category_l1.xpath(u"Группы/Группа"):
                logger.info('Category l2: %s' % category_l2.find(u'Наименование').text)
                job_args = zip([x.text for x in category_l2.xpath(u"Группы/Группа/Ссылка")],
                               [x.text for x in category_l2.xpath(u"Группы/Группа/Ид")])
                print len(job_args)
                p.map(grab_category_helper, job_args)
                logger.info('Sleep for 1 minute...')
                time.sleep(120)
def grab_product_characts():
    g = login()
    Session = sessionmaker(bind=engine)
    s = Session()
    """
    TODO в текущей версии проверка наличия характеристик проходит путем
    проверки наличия товара с данным артикулом в XML файле,
    но существуют товары, которые не имеют характеристик
    (т.к. на сайте доноре их не было во время первого парсинга), но уже записаны в XML файл.
    Таким образом, в системе появляются товары,
    которые уже никогда не получат описание
    """
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
            # for product in s.query(Product).filter(Product.category_id == '30163').all():
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
                    logger.warn('Can not open product page when parsing features for %s: %s %s' % (
                    product.articul, e.message, e.args))
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
                        etree.SubElement(new_product_characts, 'feature',
                                         {'name': ch_selector.select('td[@class="tableDataCell"]')[0].text()}).text = \
                        ch_selector.select('td[@class="tableDataCell"]')[1].text()
                    except Exception as e:
                        logger.warn('Error when parsing features for %s: %s %s' % (product.articul, e.message, e.args))
        with open(PRODUCT_CHARACTS, 'w') as f:
            f.write(etree.tostring(products_tree, pretty_print=True, encoding='utf-8'))

def delete_product(articul):
    """ Удаляем товар, а именно файл с изображением,
    описание его характеристик из xml файла, и запись из базы данных

    :param articul: артикул товара
    """
    # Удаляем характеристики товара
    products_tree = etree.parse(PRODUCT_CHARACTS)
    # Проверяем, есть ли товар в шашем катологе характеристик товаров
    expr = "/products/product[articul[text() = $articul]]"
    xml_elem = products_tree.xpath(expr, articul=articul)
    if len(xml_elem) > 0:
        xml_elem[0].getparent().remove(xml_elem[0])
        logger.info('Product %s is deleted from characteristics file' % articul)
    with open('PRODUCT_CHARACTS', 'w') as f:
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
            s.commit()
            s.close()
            logger.info('Product %s is deleted from database' % articul)
        except Exception as e:
            logger.warn('Error when deleting product %s from database: %s %s' % (articul, e.message, e.args))
    else:
        logger.info('Product %s is not present in database' % articul)

def validate_product_charcts_xml():
    # TODO функция удаляет все товары не из базы руссвет.
    products_tree = etree.parse(PRODUCT_CHARACTS)
    Session = sessionmaker(bind=engine)
    s = Session()
    for articul in products_tree.xpath("//articul"):
        product = s.query(Product).filter(Product.articul == articul.text.encode('utf-8')).first()
        if product is None:
            xml_product = articul.getparent()
            xml_product.getparent().remove(xml_product)
            logger.info('Product %s is deleted from characteristics file' % articul.text)
    with open(PRODUCT_CHARACTS, 'w') as f:
        f.write(etree.tostring(products_tree, pretty_print=True, encoding='utf-8'))


if __name__ == '__main__':
    grab_all()
    grab_product_characts()
    grab_cable_characts()
    logger.info('Creating price...')
    create_price()
    logger.info('Creating cataloge...')
    create_cataloge_csv()
    g = Grab(timeout=1200)
    g.go('http://xn---13-5cdfy6al7m.xn--p1ai/adminkrtvand/searchcron.php?full=1&token=aiCYwDnj&id_shop=1')