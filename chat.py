#!/usr/bin/env python
# -*- coding: utf-8 -*-

PROXY_LIST = '/home/andrew/proxy_list.txt'

from grab import Grab
import logging
import sys

# Зададим параметры логгирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(u'%(filename)s[LINE:%(lineno)d]# '
                              u'%(levelname)-8s [%(asctime)s]  %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logging.basicConfig(level=logging.DEBUG)



g = Grab()
with open(PROXY_LIST, 'r') as proxies_file:
    g.proxylist.load_list(proxies_file)
print g.proxylist
s2 = set()

for proxy in g.proxylist:
    try:
        logger.debug('Trying request via proxy %s' % g.config['proxy'])
        g.go('https://ru.bongacams.com')
        for link in g.doc.select('//a[@class="chat"]'):
            s2.add(link.attr('href'))
        break
    except Exception as e:
        logger.critical('Request via proxy %s failed: %s %s' % (g.config['proxy'], e.message, e.args))

s = set()
g2 = Grab()
g2.setup(proxy='194.54.64.90:5093', timeout=20)
try:
    g2.go('https://ru.bongacams.com')
    for link in g2.doc.select('//a[@class="chat"]'):
        s.add(link.attr('href'))
except Exception as e:
    logger.critical('Request failed')




with open('/home/andrew/bcams', 'w') as f:
    f.write('Query from Saransk:\n')
    f.write(repr(s) + '\n')
    f.write('Query via proxy:' + '\n')
    f.write(repr(s2) + '\n')
    f.write('Differences:' + '\n')
    for link in s2.difference(s):
        f.write('https://ru.bongacams.com' + link)
        print 'https://ru.bongacams.com' + link
