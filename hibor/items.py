# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class HiborItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title=scrapy.Field() # 标题
    source_url=scrapy.Field()# 研报 source_url
    stock_name=scrapy.Field() # 股票名称
    stock_num=scrapy.Field() # 股票代码
    share_time=scrapy.Field() # 分享时间
    paper_column=scrapy.Field()# 研报栏目
    paper_type=scrapy.Field()# 研报类型
    researcher=scrapy.Field()# 研报作者
    institution=scrapy.Field()# 研报出处
    page_sum=scrapy.Field()# 研报页数
    attention_rate=scrapy.Field()# 推荐评级
    attachment_size=scrapy.Field()# 研报大小
    provider=scrapy.Field()# 分享者
    abstract=scrapy.Field() # 摘要
    # 暂无# pdf_url=scrapy.Field()# 研报PDF url

    # conn
    conn = scrapy.Field()
