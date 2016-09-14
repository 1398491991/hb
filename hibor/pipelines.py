# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
from .settings import FILTER_ITEMS_LIST
from scrapy.exceptions import DropItem
from datetime import datetime

class HiborPipeline(object):
    def process_item(self, item, spider):
        if item['paper_column'] not in FILTER_ITEMS_LIST:
            raise DropItem('item key "paper_column" not in FILTER_ITEMS_LIST : %s'%item['paper_column'])
        else:
            logging.debug(u'进入 urls_status_1_parse 函数处理 %s'%item['source_url'])
            # urls_status_1_parse(item,self.conn,self.cur)
        return item


    def insert_db(self,item,spider):

        sql=u"INSERT INTO urls_status(create_time,update_time,source_link_id,retry_count,is_retry,url_type) " \
            u"VALUES(%s,%s,%s,%s,%s,%s)"
        insert_data=[datetime.now(),datetime.now(),
                         item['source_link_id'],item['retry_count'],
                         item['is_retry'],item['url_type']
                     ]

        sql_company_survey=u"INSERT INTO company_survey_fix(" \
                    u"create_time,update_time,title,source_link_id,source_link,stock_name,stock_num,date_time," \
                    u"paper_column,paper_type,researcher,institution,page_sum," \
                    u"attention_rate,attachment_size,provider,abstract) " \
                    u"VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        insert_data_company_survey=(datetime.now(),datetime.now(),item['title'],item['source_link_id'],item['source_url'],item['stock_name'],
                                    data['stock_num'],data['share_time'],item['paper_column'],
                     data['paper_type'],data['researcher'],data['institution'],item['page_sum'],item['attention_rate'],data['attachment_size'],
                     data['provider'],data['abstract'])

        try:
            cur.execute(sql_company_survey,insert_data_company_survey)
            insert_data[-2]=0
            cur.execute(sql,insert_data)
            conn.commit()
            logging.info('urls_status_1_parse Successful : %s'%data['source_link_id'])
            return True
        except Exception as e:
            conn.rollback()
            logging.error('urls_status_1_parse insert Error : %s  %s'%(data['source_link_id'],e))
            return False