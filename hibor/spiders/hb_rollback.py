# -*- coding: utf-8 -*-
# import scrapy
from hb_base import HbBaseSpider
import logging
import re
from ..items import HiborRollbackItem
from ..settings import fetch_sql
class HbRollbackSpider(HbBaseSpider):
    name = "hb_rollback"
    base_url = HbBaseSpider.base_url

    HbBaseSpider.cur.execute(fetch_sql)
    db_data = HbBaseSpider.cur.fetchall()

    def start_requests(self):
        for source_link_id,retry_count in self.db_data:
            meta={'my_retry_count':retry_count+1,  # 重试次数+1
                  'source_link_id':source_link_id}
            yield self._requests(self.base_url%source_link_id,meta=meta,property=100) # 优先级高一些


    def parse(self, response):
        if not response.text.strip():# 爬虫被屏蔽了
            self.log(u'shield',logging.warn)

        else:

            title=response.xpath('//div[@class="leftn2"]//h1/span/text()').extract_first()
            source_url=response.url
            stock_name=response.xpath('//table[@align="center"]/tr[1]/td[1]/span/a/text()').extract_first()
            stock_num=response.xpath('//table[@align="center"]/tr[1]/td[2]/span/a/text()').extract_first()
            share_time=response.xpath('//table[@align="center"]/tr[1]/td[3]/span/text()').extract_first()
            paper_column=response.xpath('//table[@align="center"]/tr[2]/td[1]/span/a/text()').extract_first()
            paper_type=response.xpath('//table[@align="center"]/tr[2]/td[2]/span/span/text()').extract_first()
            researcher=response.xpath('//table[@align="center"]/tr[2]/td[3]/span/a/text()').extract_first()
            institution=response.xpath('//table[@align="center"]/tr[3]/td[1]/span/a/text()').extract_first()
            page_sum=response.xpath('//table[@align="center"]/tr[3]/td[2]/span/text()').extract_first()
            attention_rate=response.xpath('//table[@align="center"]/tr[3]/td[3]/span[1]/a/text()').extract_first()
            attachment_size=response.xpath('//table[@align="center"]/tr[4]/td[1]/span[1]/text()').extract_first()
            provider=response.xpath('//table[@align="center"]/tr[4]/td[2]/span/text()').extract_first()
            abstract=response.xpath('//div[@class="p_main"]/p/font//text()').extract()
            abstract='\n'.join(abstract).replace(u'\xa0','')#.encode(response.encoding)
            abstract=re.sub("http[s]{0,1}://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+",'',abstract)
            abstract=re.sub(u'[(（【]+.*?[】）)]+','',abstract)
            meta = response.meta
            item=HiborRollbackItem()
            item['source_link_id']=meta['source_link_id']
            item['retry_count']=meta['my_retry_count']
            item['is_retry']=0 if share_time else 1

            item['title']=title
            item['source_url']=source_url
            item['stock_name']=stock_name
            item['stock_num']=stock_num
            item['share_time']=share_time
            item['paper_column']=paper_column
            item['paper_type']=paper_type
            item['researcher']=researcher
            item['institution']=institution
            item['page_sum']=page_sum
            item['attention_rate']=attention_rate
            item['attachment_size']=attachment_size
            item['provider']=provider
            item['abstract']=abstract
            item['co'] = (HbBaseSpider.conn,HbBaseSpider.cur)
            yield item
