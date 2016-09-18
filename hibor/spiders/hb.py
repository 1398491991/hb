# -*- coding: utf-8 -*-
from ..settings import STEP_PAGE
from ..items import HiborItem
from ..spider_cfg.hb_cfg import HbSpiderConfig
import re
import logging
import datetime
from hb_base import HbBaseSpider



class HbSpider(HbBaseSpider):
    name = "hb"
    # allowed_domains = []
    base_url=HbBaseSpider.base_url
    hbcfg = HbSpiderConfig() # 初始化并且读取配置文件
    start_page = hbcfg.cfg.getint(hbcfg.section,'start_page')
    end_page = hbcfg.cfg.getint(hbcfg.section,'end_page')
    if not (start_page and end_page):
        # 此时读取到的配置文件为 start_page and end_page 都为 0
        HbBaseSpider.cur.execute('SELECT max(source_link_id) from urls_status')
        start_page=HbBaseSpider.cur.fetchone()[0]+1
        end_page=start_page+STEP_PAGE
    assert (isinstance(start_page,(int,long)) and isinstance(end_page,(int,long))) is True

    start_datetime=datetime.datetime.now() # 记录爬虫开始时间

    def start_requests(self):
        yield self._requests(self.base_url%self.start_page)

    def parse(self, response):
        if not response.text.strip():# 爬虫被屏蔽了
            self.log(u'shield',logging.warn)
            kv={'start_page':self.start_page,
                'end_page':self.end_page}
            self._update_cfg(kv)

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
            item['co'] = (HbBaseSpider.conn,HbBaseSpider.cur)
            item['source_link_id']=self.start_page
            item['is_retry']=0 if share_time else 1

            yield item
            yield self._next_page()





    def _next_page(self,*args,**kwargs):
        if self.start_page<=self.end_page:
            self.start_page+=1
            next_url = self.base_url%self.start_page
            return self._requests(next_url)
        self.log('spider job done!')
        # 正常关闭爬虫 下次重新开始
        kv={'start_page':0,
            'end_page':0}

        self._update_cfg(kv)



    def _update_cfg(self,kv):
        # 结束更新配置文件
        self.log('update hb spider cfg file!')
        end_datetime=datetime.datetime.now()
        kv['end_datetime']=str(end_datetime)
        kv['start_datetime']=str(self.start_datetime)
        self.hbcfg.update(kv) # 更新
        self.hbcfg.save() # 写入

