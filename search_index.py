#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import pycurl

from grab import Grab

# Зададим параметры логгирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(u'%(filename)s[LINE:%(lineno)d]# '
                              u'%(levelname)-8s [%(asctime)s]  %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


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

g = Grab(timeout=1200)
logger.info('search index')
g.go('http://xn---13-5cdfy6al7m.xn--p1ai/adminkrtvand/searchcron.php?full=1&token=aiCYwDnj&id_shop=1')
#Вы можете установить задание cron для переиндексирования цен, используя следующий URL:
logger.info('price index')
g.go('http://xn---13-5cdfy6al7m.xn--p1ai/modules/blocklayered/blocklayered-price-indexer.php?token=b3c6a957c9')
#Вы можете установить задание cron для переиндексирования атрибутов, используя следующий URL:
logger.info('attribute index')
g.go('http://xn---13-5cdfy6al7m.xn--p1ai/modules/blocklayered/blocklayered-attribute-indexer.php?token=b3c6a957c9')
#Вы можете установить задание cron для переиндексирования URL, используя следующий URL:
logger.info('url index')
g.go('http://xn---13-5cdfy6al7m.xn--p1ai/modules/blocklayered/blocklayered-url-indexer.php?token=b3c6a957c9&truncate=1')