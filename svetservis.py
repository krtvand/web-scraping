#!/usr/bin/env python
# -*- coding: utf-8 -*-

from grab import Grab
import logging
import re

class Svetservis(object):
    def __init__(self):
        self.g = Grab()
    def get_categories(self):
        self.g.go('http://store.svetservis.ru/map/')
        category = {}
        categories_level_2 = self.g.doc.select('//div[@class="pod_cart"]/ul')
        for category_level_2 in categories_level_2:
            print 1

s = Svetservis()
s.get_categories()
