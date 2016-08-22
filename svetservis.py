#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Определяем категории, которые будем парсить для своего магазина
CATEGORIES_FROM_SVETSERVIS = {u'01 Кабельно-проводниковая продукция',
                              u'0101 Кабель провод категории ТУ',
                              u'0102 Кабель провод категории ГОСТ'}
SVETSERVIS_PRICE = '/home/andrew/svetservis_price.xls'
MY_CATEGORY_LIST = '/home/andrew/my_cataloge.xml'
HOME_FEATURED_PRODUCTS = '/home/andrew/home_featured_products.xml'
PRODUCT_CHARACTS = '/home/andrew/product_characteristics.xml'
CABLE_CHARACTS = '/home/andrew/cable_characteristics.xml'

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
from lxml import etree
from multiprocessing import Pool
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
            rb = xlrd.open_workbook(SVETSERVIS_PRICE)
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
        # TODO Записи в XML файл создаются при каждом запуске функции
        """ Получаем все категории товаров
        """
        try:
            self.g.go('http://store.svetservis.ru/map/')
        except:
            self.logger.warn('Can not access http://store.svetservis.ru/map/')

        tree = etree.parse(MY_CATEGORY_LIST)
        # Используем в качестве id категорий инкрементный индекс начиная с 10ти
        category_id = 10
        ul_level_1_selectors = self.g.doc.select('//div[@class="pod_cart"]/ul')
        category_level_1_names = self.g.doc.select('//div[@class="pod_cart"]/p/b')

        for ul_level_1_selector, category_level_1_name in zip(ul_level_1_selectors, category_level_1_names):
            #print type(category_level_1_name.text()), category_level_1_name
            if category_level_1_name.text() not in CATEGORIES_FROM_SVETSERVIS:
                continue
            category_level_1 = Category(category_level_1_name.text())
            category_level_1.id = category_id
            category_id += 1
            self.categories.append(category_level_1)
            li_index = 1
            self.logger.debug('%s (id=%s)' % (category_level_1.name, category_level_1.id))
            # Сохраняем ссылку на каталог в XML файл в атрибуте "Ссылка"
            xml_category = etree.SubElement(tree.getroot(), u"Группа")
            etree.SubElement(xml_category, u"Ид").text = str(category_id)
            etree.SubElement(xml_category, u"Наименование").text = category_level_1.name
            for category_level_2_selector in ul_level_1_selector.select('./li'):
                if category_level_2_selector.text() not in CATEGORIES_FROM_SVETSERVIS:
                    continue
                category_level_2 = Category(category_level_2_selector.text())
                category_level_2.id = category_id
                category_id += 1
                category_level_1.children.append(category_level_2)
                self.logger.debug('%s (id=%s)' % (category_level_2.name, category_level_2.id))
                # Сохраняем ссылку на каталог в XML файл в атрибуте "Ссылка"
                xml_category_level_2 = etree.SubElement(xml_category, u"Группа")
                etree.SubElement(xml_category_level_2, u"Ид").text = str(category_level_2.id)
                etree.SubElement(xml_category_level_2, u"Наименование").text = category_level_2.name
                for category_level_3_selector in ul_level_1_selector.select('./ul[' + str(li_index) + ']/li'):
                    category_level_3 = Category(category_level_3_selector.text())
                    category_level_3.id = category_id
                    category_id += 1
                    category_level_3.link = 'http://store.svetservis.ru' + category_level_3_selector.select('./a').attr('href')
                    category_level_2.children.append(category_level_3)
                    self.logger.debug('%s (id=%s) %s' % (category_level_3.name, category_level_3.id, category_level_3.link))
                    # Сохраняем ссылку на каталог в XML файл в атрибуте "Ссылка"
                    xml_category_level_3 = etree.SubElement(xml_category_level_2, u"Группа")
                    etree.SubElement(xml_category_level_3, u"Ид").text = str(category_level_3.id)
                    etree.SubElement(xml_category_level_3, u"Наименование").text = category_level_3.name
                    etree.SubElement(xml_category_level_3, u"Ссылка").text = category_level_3.link
                li_index += 1

        with open(MY_CATEGORY_LIST, 'w') as f:
            f.write(etree.tostring(tree, pretty_print=True, encoding='utf-8'))

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
        img_directory = '/var/www/html/images' + re.sub(r'[^/]*$','',img_ref)
        if not os.path.exists(img_directory):
            os.makedirs(img_directory)
        with open('/var/www/html/images' + img_ref, 'w') as img:
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
            full_img_ref = 'http://localhost/images' + img_ref
        except:
            self.logger.warning('Problems with image downloading')
            img_ref = ''
            full_img_ref = ''
        # Сохраняем товар в базу данных
        goods = Goods(articul=articul.encode('utf-8'),
                      name_from_price=name_from_price.encode('utf-8'),
                      name_from_site=title.encode('utf-8'), img_ref=full_img_ref.encode('utf-8'))
        return goods
        # Получаем ссылку на каталог товаров в виде прайс листа
        #print self.g.doc.select(u'//div[@class="col-2"]//a[contains(.,"Прайс-лист каталога")]').attr('title')

    def scrap_category(self, category_link, category_id):
        """ Парсим одну категорию 3 уровня
        :param category: экземпляр класса "Категория"
        :return: В случае ошибки возвращает None, в случае успеха ничего не возвращает
        """
        g = Grab()
        try:
            g.go(category_link)
        except:
            self.logger.warn('Can not access %s' % category_link)
            return None
        # Переходим по ссылке "Все позиции", если в категории меньше 10 позиций,
        #  то ссылка отсутствует, поэтому используем изначальную ссылку на категорию
        try:
            category_all_ref = g.doc.select(u'//a[@title="Все позиции"]').attr('href')
        except:
            self.logger.debug('Data not found for link "All goods"')
            category_all_ref = category_link
        try:
            g.go(category_all_ref)
        except:
            self.logger.warn('Can not access %s' % category_link)
            return None
        try:
            cards = g.doc.select('//div[@class="IndexSpecMainDiv"]/table/tr/td/table')
        except:
            self.logger.critical('Can not get cards for goods in category %s' % category_link)
            return None
        # Обходим все товары в категории и сразу сохраняем их в БД
        for card in cards:
            goods = self.get_goods(card)
            if goods is not None:
                goods.category = category_id.encode('utf-8')
                s = self.session()
                s.merge(goods)
                s.commit()

    def scrap_all(self):
        """ Парсим весь сайт, проходя через каждую категорию 3 уровня
        """
        """
        #self.get_categories()
        for category in self.categories:
            print category.name
            if category.name in CATEGORIES_FROM_SVETSERVIS:
                for it in category.children:
                    print it.name
                    if it.name in CATEGORIES_FROM_SVETSERVIS:
                        for el in it.children:
                            self.scrap_category(el)
        """
        tree = etree.parse(MY_CATEGORY_LIST)
        root = tree.getroot()
        category_l1_elems = tree.xpath(u"/Группы/Группа")

        p = Pool(10)
        for category_l1 in category_l1_elems[:1]:
            category_l1_name = category_l1.find(u'Наименование').text
            self.logger.info('(%s/%s) Category l1: %s ' % (category_l1_elems.index(category_l1),
                                                      len(category_l1_elems), category_l1_name))
            for category_l2 in category_l1.xpath(u"Группы/Группа"):
                self.logger.info('Category l2: %s' % category_l2.find(u'Наименование').text)
                for category in category_l2.xpath(u"Группы/Группа"):
                    self.scrap_category(category.find(u"Ссылка").text, category.find(u"Ид").text)


    def create_csv(self):
        with open('/home/andrew/svetservis_price.csv', 'wb') as f:
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
            s = self.session()
            #  Проходим по категориям, которые есть в сокращенном списке категорий,
            # если проходить по всем товарам в базе данных,
            # то в прайс включаются товары с устаревшими категориями,
            # которые были добавлены ранее
            tree = etree.parse(MY_CATEGORY_LIST)
            characts_tree = etree.parse(PRODUCT_CHARACTS)
            home_featured_products_tree = etree.parse(HOME_FEATURED_PRODUCTS)
            for category_id in tree.xpath(u"//Ид"):
                for product in s.query(Goods).filter(Goods.category == category_id.text.encode('utf-8')).all():
            #for goods in session.query(Goods):
                    if product.wholesale_price and product.articul and \
                            product.name_from_site:
                        row = ['' for x in range(46)]
                        row[0] = product.articul.encode('utf-8')
                        row[1] = '1'
                        # Prestashop допускает максимальную длину наименования
                        # не более 128 символов
                        if len(product.name_from_site) > 128:
                            product.name_from_site = product.name_from_site.decode('utf-8')
                            product.name_from_site = product.name_from_site[0:127]
                            product.name_from_site = product.name_from_site.encode('utf-8')
                        # Исключаем/меняем недопустимые символы в названии товара
                        product.name_from_site = re.sub('=', '-', product.name_from_site)
                        product.name_from_site = re.sub(';', ',', product.name_from_site)
                        row[2] = product.name_from_site
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
                        row[23] = 10
                        row[38] = 1
                        # Временно, при следующем парсинге убрать IP
                        row[42] = product.img_ref
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
                                self.logger.warn(
                                    'Error when reading features for %s: %s %s' % (product.articul, e.message, e.args))
                        wr.writerow(row)

    def create_cataloge_csv(self):
        with open('/home/andrew/svetservis_cataloge.csv', 'wb') as f:
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

    def select_cable_characts(self, product):
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
        def write_number_wires(xml_elem):
            # Определяем количество жил и сечение
            """
                .*?\s - пропускаем название
                (\d,?\d*)\*? - первая группа количество жил и символ звездочка(может отсутствовать),
                если в имени не указано количество жил, то в группу записывается сечение
                \s? - возможен пробел после звездочки
                (\d*,?\d*)? - вторая группа сечение (например 1,5) обязательная группа, может отсутствовать
                """
            match_obj = re.search(r'.*?\s(\d,?\d*)\*?\s?(\d*,?\d*)?', product.name_from_price)
            if match_obj is not None:
                if len(match_obj.groups()) == 2:
                    # Если в наименовании указано количество жил
                    if match_obj.group(1) and len(match_obj.group(2)) > 0:
                        etree.SubElement(xml_elem.xpath('features')[0], 'feature',
                                         {'name': u'Количество жил'}).text = match_obj.group(1)
                        # В сечении кабеля меняем запятую на точку для облегчения формирования csv файла
                        etree.SubElement(xml_elem.xpath('features')[0], 'feature',
                                         {'name': u'Сечение жилы'}).text = re.sub(',', '.', match_obj.group(2))

                    elif match_obj.group(1) and len(match_obj.group(2)) == 0:
                        etree.SubElement(xml_elem.xpath('features')[0], 'feature',
                                         {'name': u'Количество жил'}).text = '1'
                        etree.SubElement(xml_elem.xpath('features')[0], 'feature',
                                         {'name': u'Сечение жилы'}).text = re.sub(',', '.', match_obj.group(1))
        def write_model(xml_elem):
            # Определяем марку кабеля как пересечение возможных
            # типов кабеля с каждым словом из названия продукта
            split_name = set(re.split('[\s/]', product.name_from_price.decode('utf-8')))
            try:
                etree.SubElement(xml_elem.xpath('features')[0], 'feature', {'name': u'Марка'}).text = (
                    characts_set & split_name).pop()
            except:
                # В случае, если в наименовании товара отсутствует марка кабеля из списка,
                # мы получаем ошибку 'pop from an empty set'. Данную ошибку пропускаем.
                pass
                # xml_elem[0].xpath('characteristics/model').text = 'test'
        if len(xml_elem) > 0:
            # Количество жил и сечение
            if xml_elem[0].xpath(u'features/feature[@name = "Количество жил"]'):
                pass
            else:
                write_number_wires(xml_elem[0])
                with open(PRODUCT_CHARACTS, 'w') as f:
                    f.write(etree.tostring(products_tree, pretty_print=True, encoding='utf-8'))
            # Добавляем марку кабеля
            if xml_elem[0].xpath(u'features/feature[@name = "Марка"]'):
                return True
            else:
                write_model(xml_elem[0])
                with open(PRODUCT_CHARACTS, 'w') as f:
                    f.write(etree.tostring(products_tree, pretty_print=True, encoding='utf-8'))
        else:
            new_product = etree.SubElement(products_tree.getroot(), "product")
            etree.SubElement(new_product, "articul").text = product.articul
            etree.SubElement(new_product, "name").text = product.name_from_price.decode('utf-8')
            etree.SubElement(new_product, "features")
            write_number_wires(new_product)
            write_model(new_product)
            with open(PRODUCT_CHARACTS, 'w') as f:
                f.write(etree.tostring(products_tree, pretty_print=True, encoding='utf-8'))




    def grab_cable_characts(self):
        """ Получаем марку кабеля для все товаров из категории "Кабели и провода"
        """
        tree = etree.parse(MY_CATEGORY_LIST)
        s = self.session()
        # Получаем все id подкатегорий Кабели и провода (нулевой в списке категорий)
        for category_id in tree.xpath(u"/Группы/Группа")[0].xpath(u"Группы/Группа/Группы/Группа/Ид"):
            for product in s.query(Goods).filter(Goods.category == category_id.text).all():
                self.select_cable_characts(product)
                print product.name_from_price
ss = Svetservis()
#s.scrap_all()
ss.read_price()
#ss.grab_cable_characts()
ss.create_csv()
