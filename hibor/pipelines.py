# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
from .settings import FILTER_ITEMS_LIST
from scrapy.exceptions import DropItem

class HiborPipeline(object):
    def process_item(self, item, spider):
        if item['paper_column'] not in FILTER_ITEMS_LIST:
            raise DropItem('paper_column not in FILTER_ITEMS_LIST : %s'%item['paper_column'])
        else:
            logging.debug(u'进入 urls_status_1_parse 函数处理 %s'%item['source_url'])
            # urls_status_1_parse(item,self.conn,self.cur)
        return item
