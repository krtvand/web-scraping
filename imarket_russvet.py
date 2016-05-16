#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys

from grab import Grab
import pycurl

class Category(object):
    def __init__(self, name):
        self.id = None
        self.name = name
        self.link = ''
        self.children = []

# Зададим параметры логгирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(u'%(filename)s[LINE:%(lineno)d]# '
                              u'%(levelname)-8s [%(asctime)s]  %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

g = Grab()
g.transport.curl.setopt(pycurl.HEADER, True)
g.transport.curl.setopt(pycurl.SSLCERT, '/scripts/web-scraping/rus-svet-cert.pem')
g.transport.curl.setopt(pycurl.SSLCERTPASSWD, 'null95f353bd')
g.go('https://imarket.russvet.ru:5000')
redirect_link = g.doc.select('//a').attr('href')
logger.debug('redirect link %s' % redirect_link)
resp = g.go(redirect_link)
g.doc.set_input_by_id('username', 'krtvand')
g.doc.set_input_by_id('password', 'krtvand')
resp = g.doc.submit()
category_level_1_selectors = g.doc.select('//td[@class="customCategory"]//a[@class="levelOne"]')
categories = Category('top')
for category_level_1_selector in category_level_1_selectors:
    category_level_1 = Category(category_level_1_selector.text())
    logger.debug('Level 1 %s' % category_level_1_selector.text())
    category_level_1.link = category_level_1_selector.attr('href')
    logger.debug(category_level_1_selector.attr('href'))
    categories.children.append(category_level_1)

for category_level_1 in categories.children:
    g.go(category_level_1.link)
    for category_level_2_selector in g.doc.select('//td[@class="customCategory"]//a[@class="levelTwo"]'):
        category_level_2 = Category(category_level_2_selector.text())
        logger.debug('Level 2 %s' % category_level_2_selector.text())
        category_level_2.link = category_level_2_selector.attr('href')
        logger.debug(category_level_2_selector.attr('href'))
        category_level_1.children.append(category_level_2)

with open('/home/andrew/temp.html', 'w') as f:
    f.write(resp.body)
print 'ok'
