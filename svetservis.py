#!/usr/bin/env python
# -*- coding: utf-8 -*-

from grab import Grab
import logging
import re
import sys

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
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter(u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
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
                    category_level_3.link = ul_level_1_selector.select('./ul[' + str(li_index) + ']/li/a').attr('href')
                    category_level_2.children.append(category_level_3)
                    self.logger.debug(category_level_3_selector.text())
                li_index += 1


s = Svetservis()
s.get_categories()
for category in s.categories:
    print '%s %s %s ' % (category.name, category.link, len(category.children))
    for it in category.children:
        print '%s %s %s ' % (it.name, it.link, len(it.children))
        for el in it.children:
            print '%s %s %s ' % (el.name, el.link, len(el.children))