# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class DangdangItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    b_cate = scrapy.Field()
    m_cate = scrapy.Field()
    s_cate = scrapy.Field()
    s_href = scrapy.Field()
    book_img = scrapy.Field()
    book_name = scrapy.Field()
    book_url = scrapy.Field()
    book_desc = scrapy.Field()
    book_price = scrapy.Field()
    book_author = scrapy.Field()
    book_publish_data = scrapy.Field()
    book_press = scrapy.Field()
    book_comment = scrapy.Field()

