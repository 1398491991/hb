# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
from .settings import FILTER_ITEMS_LIST
# from scrapy.exceptions import DropItem
from datetime import datetime
logger = logging.getLogger(__name__)

class HiborBasePipeline(object):
    sql_urls_status_update=u"update urls_status set update_time=%s,retry_count=%s,is_retry=%s where source_link_id=%s"

    sql_urls_status_insert=u"INSERT INTO urls_status(create_time,update_time,source_link_id,retry_count,is_retry,url_type) " \
            u"VALUES(%s,%s,%s,%s,%s,%s)"

    _sql_company_survey_base=u"(" \
                    u"create_time,update_time,title,source_link_id,source_link,stock_name,stock_num,date_time," \
                    u"paper_column,paper_type,researcher,institution,page_sum," \
                    u"attention_rate,attachment_size,provider,abstract) " \
                    u"VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    sql_company_survey_fix = u"INSERT INTO company_survey_fix"+_sql_company_survey_base
    sql_company_survey_fix_other = u"INSERT INTO company_survey_fix_other"+_sql_company_survey_base

    def process_item(self, item, spider):
        raise NotImplementedError


    def _insert_company_survey_fix(self,cur,item):
        self._tmp_insert_company_survey(item,self.sql_company_survey_fix,cur)

    def _insert_company_survey_fix_other(self,cur,item):
        self._tmp_insert_company_survey(item,self.sql_company_survey_fix_other,cur)

    def _tmp_insert_company_survey(self,item,sql,cur):
        if not item['is_retry']:
            insert_data_company_survey=(datetime.now(),datetime.now(),item['title'],item['source_link_id'],item['source_url'],item['stock_name'],
                            item['stock_num'],item['share_time'],item['paper_column'],
                         item['paper_type'],item['researcher'],item['institution'],item['page_sum'],item['attention_rate'],item['attachment_size'],
                         item['provider'],item['abstract'])
            self._execute_sql(cur,sql,insert_data_company_survey)

    def _update_urls_status(self,cur,item):
        update_data=(datetime.now(),item['retry_count'],item['is_retry'],item['source_link_id'])
        self._execute_sql(cur,self.sql_urls_status_update,update_data)


    def _insert_urls_status(self,cur,item):
        insert_data=(datetime.now(),datetime.now(),
                 item['source_link_id'],item.get('retry_count',1),
                 item['is_retry'],item.get('url_type',1)
                     )
        self._execute_sql(cur,self.sql_urls_status_insert,insert_data)


    def _execute_sql(self,cur,sql,param):
        cur.execute(sql,param)



class HiborPipeline(HiborBasePipeline):
    """每日更新操作到数据库"""
    def process_item(self, item, spider):
        if spider.name == 'hb':
            conn,cur=item['co']
            try:
                if item['paper_column'] in FILTER_ITEMS_LIST:
                    logging.debug('parse item... , from url : %s'%item['source_url'])
                    self._insert_company_survey_fix(cur,item)
                    self._insert_urls_status(cur,item)
                else:
                    # 插入 company_survey_fix_other 表中
                    logging.debug('item key "paper_column" not in FILTER_ITEMS_LIST : %s , %s'%(item['paper_column'],item['source_url']))
                    self._insert_company_survey_fix_other(cur,item)

                conn.commit()
                logging.info('insert db successful : %s'%item['source_link_id'])

            except Exception as e:
                conn.rollback()
                logging.error('insert db error : %s  , %s'%(item['source_link_id'],e))

            finally:
                return item







class HiborRollBackPipeline(HiborBasePipeline):
    """
    回滚数据库 重新抓取未抓取到的数据
    """
    def process_item(self, item, spider):
        if spider.name == 'hb_rollback':
            conn,cur=item['co']
            logging.debug(u'update db... , from url : %s'%item['source_url'])
            try:
                self._update_urls_status(cur,item)
                self._insert_company_survey_fix(cur,item)
                conn.commit()
                logging.info('update db successful : %s'%item['source_link_id'])

            except Exception as e:
                conn.rollback()
                logging.error('update db error : %s  , %s'%(item['source_link_id'],e))

            finally:
                return item
