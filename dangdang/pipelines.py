# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import pymysql


class MongoPipeline(object):

    def __init__(self, mongo_url, mongo_db):
        self.mongo_url = mongo_url
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_url=crawler.settings.get('MONGO_URL'),
            mongo_db=crawler.settings.get('MONGO_DB')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_url)
        self.db = self.client[self.mongo_db]

    def process_item(self, item, spider):
        name = item.__class__.__name__
        self.db[name].insert(dict(item))
        return item

    def close_spider(self, spider):
        self.client.close()

class MysqlPipeline(object):
    def __init__(self):
        # 连接MySQL数据库
        self.connect = pymysql.connect(host='47.115.21.129', user='root', password='111111', port=3306)
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        try:
            self.cursor.execute('create database jd charset="utf8"')
            print('创建数据库成功Daatabase created')
        except:
            # print('Database jd exists!')
            pass
        self.connect.select_db('jd')
        try:
            self.cursor.execute('create table dangdang(id int AUTO_INCREMENT PRIMARY KEY, b_cate VARCHAR(10) NULL comment "一类", '
                                'm_cate VARCHAR(10) NULL comment "二类", s_cate VARCHAR(20) NULL comment "三类", s_href VARCHAR(100) NULL comment "类别URL",'
                                'book_img  VARCHAR(100) NULL comment "图书图片", book_name  VARCHAR(300) NULL comment "图书名字", book_url VARCHAR(100) NULL comment "图书URL",'
                                'book_desc  VARCHAR(500) NULL comment "图书简介" , book_price  VARCHAR(20) NULL comment "图书价格", book_author VARCHAR(100) NULL comment "图书作者",'
                                'book_publish_date VARCHAR(10) NULL comment "图书出版时间", book_press VARCHAR(20) NULL comment "出版社", book_comment VARCHAR(10)  NULL comment "图书评论数据")')
            print('创建数据表成功Tables created')
        except:
            # print('The table dangdang exists!')
            pass
        # 往数据库里面写入数据
        self.cursor.execute(
            'insert into dangdang(b_cate, m_cate, s_cate, s_href, book_img, book_name, book_url, book_desc, book_price, book_author, book_publish_date, book_press, book_comment)VALUES ("{}","{}","{}","{}","{}","{}","{}","{}",'
            '"{}","{}","{}","{}","{}")'.format(item['b_cate'], item['m_cate'], item['s_cate'], item['s_href'], item['book_img'], item['book_name'], item['book_url'],
                                               item['book_desc'], item['book_price'], item['book_author'], item['book_publish_data'], item['book_press'], item['book_comment']))
        
        print(item)
        self.connect.commit()
        return item

    # 关闭数据库
    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()
