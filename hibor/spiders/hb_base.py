#coding=utf-8
import scrapy
import MySQLdb
from ..settings import DB_CONFIG

class HbBaseSpider(scrapy.Spider):
    base_url='http://www.hibor.com.cn/docdetail_%s.html'
    conn=MySQLdb.connect(**DB_CONFIG)
    cur=conn.cursor()

    def _requests(self, url,*args,**kwargs):
        return scrapy.Request(url,*args,**kwargs)

    @staticmethod
    def close(spider, reason):
        closed = getattr(spider, 'closed', None)
        try:
            spider.conn.close()
            spider.log('database conn close!')
        except:
            pass
        if callable(closed):
            return closed(reason)