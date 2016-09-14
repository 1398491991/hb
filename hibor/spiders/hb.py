# -*- coding: utf-8 -*-
import scrapy
import MySQLdb
from ..settings import DB_CONFIG,CONTINUOUS_PAGE
from ..items import HiborItem
from ..cfg import SpiderConfig
import re
import logging

class HbSpider(scrapy.Spider):
    name = "hb"
    # allowed_domains = []
    base_url='http://www.hibor.com.cn/docdetail_%s.html'
    start_urls = (
        base_url,
    )
    scfg=SpiderConfig() #读取爬虫的配置文件
    scfg_dicts = dict(scfg.getitems('spider config'))

    conn=MySQLdb.connect(**DB_CONFIG)
    cur=conn.cursor()

    cur.execute('SELECT max(source_link_id) from urls_status')
    cur_start_page=cur.fetchone()

    config_start_page = scfg_dicts.get('start_page')
    try:
        continuous_page = int(scfg_dicts['continuous_page'])
    except:
        logging.warn('local_settings.ini continuous_page key not found')
        continuous_page = CONTINUOUS_PAGE

    if config_start_page and config_start_page>0:
        start_page = int(config_start_page)
    else:
        start_page = cur_start_page[0]



    def start_requests(self):
        for x in HbSpider.start_urls:
            yield scrapy.Request(x%HbSpider.start_page)

    def parse(self, response):
        if not response.text.strip():# 爬虫被屏蔽了
            self.log(u'shield',logging.warn)
            HbSpider.scfg.update(HbSpider.config_start_page,HbSpider.start_page,0)

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
            if not share_time:# 无此网页
                HbSpider.continuous_page -= 1
            else:
                item=HiborItem()
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
                item['conn'] = HbSpider.conn
                yield item

            if HbSpider.continuous_page>0:
                HbSpider.start_page += 1
                yield scrapy.Request(HbSpider.base_url%HbSpider.start_page,)
            else:# 本次爬去完毕
                HbSpider.scfg.update('','',1)




    @staticmethod
    def close(spider, reason):
        closed = getattr(spider, 'closed', None)
        spider.log(u'database conn close')
        HbSpider.conn.close()
        if callable(closed):
            return closed(reason)