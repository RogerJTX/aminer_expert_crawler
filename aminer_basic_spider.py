#coding=utf8
"""
url：https://www.aminer.cn/
https://www.aminer.cn/profile/yueting-zhuang/54298b7adabfaec7081904c4
author: jtx
date: 2020_05_03
"""

import sys
import os
import re
from bs4 import BeautifulSoup
import logging
import pymongo
import base64
import urllib
import time, requests
import datetime, random
from etl.utils.log_conf import configure_logging
import traceback
from etl.data_gather.settings import SAVE_MONGO_CONFIG, RESOURCE_DIR
from etl.common_spider.donwloader import Downloader
import json





class ListDetailSpider(object):
    def __init__(self, config, proj=None):
        config["db"] = 'automobile_kb'
        self.proj = proj
        self.host = "www.aminer.cn"  # 网站域名
        self.host2 = "www.aminer.cn"
        self.host_name = "Aminer"  # 网站中文名
        self.api_url = "https://www.aminer.cn/profile/"  # 起始URL或者是基础URL，请求的链接在此基础生成
        self.mongo_client = self.get_mongo(**config)
        self.save_coll_name = "expert_list"  # 需要保存的表名
        self.mongo_db = self.mongo_client[config["db"]]

        config["db1"] = 'automobile_kb'
        self.read_col1_name = "expert_aminer"
        self.mongo_read_db1 = self.mongo_client[config["db1"]]

        # config["db2"] = 'res_kb'
        # self.read_col2_name = "res_kb_expert_relation"
        # self.mongo_read_db2 = self.mongo_client[config["db2"]]

        # config["db3"] = 'res_kb'
        # self.read_col3_name = "industry_keyword_expert_name"
        # self.mongo_read_db3 = self.mongo_client[config["db3"]]

        # config["db4"] = 'res_kb'
        # self.read_col4_name = "aminer_maintenance_table"
        # self.mongo_read_db4 = self.mongo_client[config["db4"]]


        self.start_down_time = datetime.datetime.now()
        self.down_retry = 5
        configure_logging("AMINER_R2.log")  # 日志文件名
        self.logger = logging.getLogger("spider")
        self.downloader = Downloader(self.logger, need_proxy=False)  # 注意是否需要使用代理更改参数
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0",
        }
        self.headers2 = {'Host': self.host,
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:69.0) Gecko/20100101 Firefox/69.0'}
        self.count = 0
        # 链接mongodb

    def get_mongo(self, host, port, db, username, password):
        if username and password:
            url = "mongodb://%s:%s@%s:%s/%s" % (username, password, host, port, db)
        else:
            url = "mongodb://%s:%s" % (host, port)
        return pymongo.MongoClient(url)

    def save_record(self, record, coll_name, pk):

        tmp = []
        self.count += 1
        print('count:', self.count)

        for k, v in pk.items():
            tmp.append("%s=%s" % (k, v))
            # print( tmp
        show = "  ".join(tmp)
        r_in_db = coll_name.find_one(pk)
        if not r_in_db:
            coll_name.insert_one(record)
            self.logger.info("成功插入(%s)" % (record['id']))
        else:
            self.logger.info("重复数据(%s)" % (record['id']))

        self.update_maintenance_table(flag_aminer=1, flag_aminer_result=1)

    def url_list_page(self, content_page_list, page_size):
        records = []
        content_json = json.loads(content_page_list)
        # 判断是否有返回结果
        data = content_json['data']

        if 'pager' in data[0]:

            pager = content_json['data'][0]['pager']
            num_total = int(pager['total'])
            page_size = int(page_size)
            if num_total < page_size:
                page_num = 1
            else:
                if num_total % page_size == 0:
                    page_num = num_total / page_size
                else:
                    page_num = int(num_total / page_size) + 1
            self.logger.info('总共结果%s, 页数%s'% (num_total, page_num))
            persons = content_json['data'][0]['persons']
            for i in persons:
                img_url = ''
                id = ''
                indices = []
                name_zh = ''
                name_en = ''
                profile = ''
                affiliation = ''
                affiliation_zh = ''
                position = ''
                org = ''
                if 'avatar' in i:
                    img_url = i['avatar']
                if 'id' in i:
                    id = i['id']
                if 'indices' in i:
                    indices = i['indices']
                if 'name_zh' in i:
                    name_zh = i['name_zh']
                if 'name' in i:
                    name_en = i['name']
                if 'profile' in i:
                    profile = i['profile']
                    if 'affiliation' in profile:
                        affiliation = profile['affiliation']
                    if 'affiliation_zh' in profile:
                        affiliation_zh = profile['affiliation_zh']
                    if 'position' in profile:
                        position = profile['position']
                    if 'org' in profile:
                        org = profile['org']
                records.append({'expert_name_zh':name_zh, 'expert_name_en':name_en, 'professional_title':position,
                            'research_institution_en':affiliation, 'research_institution_zh':affiliation_zh,
                            'org':org, 'img_url':img_url, 'id':id, 'indices':indices })

            return records, page_num



        else:
            self.logger.info('查询无返回结果:%s' % self.expert_name)
            page_num = 0
            records = []
            return records, page_num

    def url_detail_page(self, content_detail_page, info, url_detail_page):
        record = {}
        record.update(info)

        # 获奖
        url_award = 'https://api.aminer.cn/api/person/award-tags/'+self.id+'/offset/0/size/100'
        resp_award = self.downloader.crawl_data(url_award, None, self.headers, "get")
        awards_list = []
        if resp_award:
            resp_award.endocing = 'utf-8'
            content_award = resp_award.text
            json_award = json.loads(content_award)
            awards = json_award['awards']
            if awards:
                for each_a in awards:
                    awards = each_a['l']
                    awards_list.append(awards)
        record['awards'] = awards_list



        json_content = json.loads(content_detail_page)
        i = profile = json_content['profile']['profile']
        indices = []
        nation = ''
        research_field_tags = []
        research_field_tags_score = []
        keyword_zh = []
        email = ''
        fax = ''
        gender = ''
        phone = ''
        work_experience = ''
        lang = ''
        homepage = ''
        expert_resume = ''
        education_experience = ''
        address = ''

        if i:
            # indices = i['indices']
            if 'nation' in i:
                nation = i['nation']
            else:
                nation = ''

            address = ''
            if 'tags' in i:
                research_field_tags = i['tags']
            if 'tags_score' in i:
                research_field_tags_score = i['tags_score']
            if 'tags_zh' in i:
                keyword_zh = i['tags_zh']
            else:
                keyword_zh = []

            i2 = profile2 = profile.get('profile', '')
            if i2:
                email = i2.get('email', '')
                fax = i2.get('fax', '')
                gender = i2.get('gender', '')
                phone = i2.get('phone', '')
                work_experience = i2.get('work', '')
                lang = i2.get('lang', '')
                homepage = i2.get('home', '')
                expert_resume = i2.get('bio', '')
                education_experience = i2.get('edu', '')
                address = i2.get('address', '')




        # record['indices'] = indices
        record['nation'] = nation
        record['url'] = url_detail_page
        record['address'] = address
        record['research_field_tags'] = research_field_tags
        record['research_field_tags_score'] = research_field_tags_score
        record['keyword_zh'] = keyword_zh
        record['email'] = email
        record['fax'] = fax
        record['gender'] = gender
        record['phone'] = phone
        record['work_experience'] = work_experience
        record['lang'] = lang
        record['homepage'] = homepage
        record['expert_resume'] = expert_resume
        record['education_experience'] =education_experience
        record['crawl_time'] = datetime.datetime.now()

        record['source'] = '专家详情页'

        return record

    # 更新维护表
    def update_maintenance_table(self, flag_aminer, flag_aminer_result):
        # flag = 1
        myquery = {"kId": self.kId}
        newvalues = {"$set": {"flag_aminer": flag_aminer,
                              "flag_aminer_result": flag_aminer_result}}
        self.mongo_coll.update_one(myquery, newvalues)
        self.logger.info('维护表成功更新, kId:%s' % self.kId)



    def url_graph(self, content_graph):
        list_record_graph = []
        json_content_graph = json.loads(content_graph)
        data = json_content_graph['data'][0]
        if 'data' in data:
            count = data['count']
            if count != '0':
                data_list = data['data']
                for num, i in enumerate(data_list):
                    if num < 1:
                        continue
                    record_graph = {}
                    record_graph['id'] = self.id
                    record_graph['expert_name_en'] = self.expert_name_en
                    record_graph['expert_name_zh'] = self.expert_name_zh
                    record_graph['contactor_name_zh'] = i['name_zh']
                    record_graph['contactor_name_en'] = i['name']
                    record_graph['contactor_id'] = i['id']
                    record_graph['h_index'] = i['h_index']
                    record_graph['cooperative_papers_num'] = i['w']
                    url = 'https://www.aminer.cn/profile/'+i['id']
                    record_graph['url'] = url
                    record_graph['crawl_time'] = datetime.datetime.now()
                    record_graph['source'] = '关系'

                    list_record_graph.append(record_graph)

        return list_record_graph





    def url_article(self, content_article):
        list_record_article = []

        page_size = 500
        page_num = 1
        json_content_article = json.loads(content_article)
        data = json_content_article['data'][0]
        if 'keyValues' in data:
            total_num_page = data['keyValues']['total']
            self.logger.info('论文总共:%s' % str(total_num_page))
            if int(total_num_page) < page_size:
                page_num = 1
            else:
                if int(total_num_page) % page_size == 0:
                    page_num = int(total_num_page) / page_size
                else:
                    page_num = (int(total_num_page) / page_size) + 1
            items = data.get('items','')

            if items:
                for i in items:
                    # print(i)
                    record_article = {}
                    record_article['expert_id'] = self.id
                    record_article['expert_name_zh'] = self.expert_name_zh
                    record_article['expert_name_en'] = self.expert_name_en
                    record_article['id'] = i['id']
                    record_article['url'] = 'https://www.aminer.cn/pub/'+i['id']
                    if 'authors' in i:
                        record_article['authors'] = i['authors']
                    else:
                        record_article['authors'] = []
                    if 'title' in i:
                        record_article['title'] = i['title']
                    else:
                        record_article['title'] = ''
                    if 'urls' in i:
                        record_article['urls'] = i['urls']
                    else:
                        record_article['urls'] = []
                    # 引用数
                    if 'num_citation' in i:
                        record_article['num_citation'] = i['num_citation']
                    else:
                        record_article['num_citation'] = ''
                    venue_pages = {}
                    venue_list = []
                    venue = ''
                    if 'venue' in i:
                        venue_list = []
                        venue1 = i['venue']
                        # print(venue)
                        if 'info' in venue1:
                            venue1 = venue1['info']
                            if 'name' in venue1:
                                venue = venue1['name']
                                if 'pages' in i:
                                    venue_pages = i['pages']

                    venue_list.append(venue_pages)
                    venue_list.append(venue)
                    record_article['venue'] = venue_list


                    if 'lang' in i:
                        record_article['lang'] = i['lang']
                    else:
                        record_article['lang'] = ''
                    list_record_article.append(record_article)
                    record_article['year'] = i['year']
                    record_article['source'] = '论文'
                    record_article['crawl_time'] = datetime.datetime.now()
            return list_record_article, page_num

        else:

            return list_record_article, page_num




    def run(self, start_page=1, max_page=-1, page_size='20', round=1):
        """
        数据采集主入口
        :return:
        """
        self.logger.info("Begin Run")
        # ============主页面获取==============================

        # self.expert_name = '庄越挺'

        list_mongo = []
        c = 0
        for num, each in enumerate(self.mongo_coll.find()):
            print(num)
            dict_mongo = {}
            # print(each)
            search_key1 = each['expert_name']
            search_key = search_key1
            research_institution = each['research_institution']
            research_institution_en = each['research_institution_en']
            kId = each['kId']
            flag = each['flag_aminer']

            if flag == 0:
                    dict_mongo['search_key'] = search_key
                    dict_mongo['research_institution'] = research_institution
                    dict_mongo['research_institution_en'] = research_institution_en
                    dict_mongo['kId'] = kId
                    dict_mongo['flag_aminer'] = flag
                    list_mongo.append(dict_mongo)


        # with open('process_expert.txt', 'r', encoding='utf-8') as f:
        #     for line in f.readlines():
        #         if '\t' in line:
        #             line_clean = line.split('\t')[0]
        #             dict_mongo = {}
        #             print(line_clean)
        #             search_key = line_clean
        #             category = ''
        #             dict_mongo['search_key'] = search_key
        #             dict_mongo['category'] = category
        #             list_mongo.append((dict_mongo))
        c_number = 0
        flag = 0
        for each_list_mongo in list_mongo:
            self.logger.info(each_list_mongo)
            c_number += 1
            self.logger.info('第几个搜索关键词：%s' % str(c_number))

            search_key = each_list_mongo['search_key']
            research_institution = each_list_mongo['research_institution']
            research_institution_en = each_list_mongo['research_institution_en']
            kId = each_list_mongo['kId']
            flag = each_list_mongo['flag_aminer']
            # 查询部分
            # if search_key == "欧阳明高":
            #     flag = 1

            if flag == 0:
                time.sleep(1)
                self.expert_name = search_key
                self.research_institution = research_institution
                self.research_institution_en = research_institution_en
                self.kId = kId
                # 列表页
                list_data = []
                # url_list_page = 'https://apiv2.aminer.cn/n?a=__searchapi.SearchPerson___'
                # url_list_page_post_data = '[{"action":"searchapi.SearchPerson","parameters":{"offset":0,"size":2,"query":"","include":["agg","intelli","topics"],"name":"'+self.expert_name+'","org":"'+self.research_institution+'","aggregation":["gender","h_index","nation","lang"]},"schema":{"person":["id","name","name_zh","avatar","tags","is_follow","num_view","num_follow","is_upvoted","num_upvoted","is_downvoted","bind",{"profile":["position","position_zh","affiliation","affiliation_zh","org"]},{"indices":["hindex","gindex","pubs","citations","newStar","risingStar","activity","diversity","sociability"]},"tags_translated_zh"]}}]'
                url_list_page = 'https://apiv2.aminer.cn/n?a=__searchapi.SearchPerson___'
                url_list_page_post_data = '[{"action":"searchapi.SearchPerson","parameters":{"offset":0,"size":' + page_size + ',"query":"' + self.expert_name + '","include":["agg","intelli","topics"],"aggregation":["gender","h_index","nation","lang"]},"schema":{"person":["id","name","name_zh","avatar","tags","is_follow","num_view","num_follow","is_upvoted","num_upvoted","is_downvoted","bind",{"profile":["position","position_zh","affiliation","affiliation_zh","org"]},{"indices":["hindex","gindex","pubs","citations","newStar","risingStar","activity","diversity","sociability"]},"tags_translated_zh"]}}]'

                # print(url_list_page_post_data)
                resp_list_page = self.downloader.crawl_data(url_list_page, url_list_page_post_data.encode("utf-8").decode("latin1"), self.headers, "post")
                if resp_list_page:
                    resp_list_page.encoding = 'utf-8'
                    content_list_page = resp_list_page.text
                    list_data, num_page_total = self.url_list_page(content_list_page, page_size)

                else:
                    self.logger.info('列表页不响应')

                if list_data:
                    flag_end = 0
                    for result_e_num, info in enumerate(list_data):
                        # self.logger.info('搜索列表结果第几个专家:%s' % result_e_num)
                        id = info['id']
                        self.expert_name_zh = info['expert_name_zh']
                        self.expert_name_en = info['expert_name_en']
                        self.id = id


                        if (self.expert_name_zh == self.expert_name) and ((self.research_institution in info['research_institution_zh']) or (self.research_institution in info['research_institution_en']) or (self.research_institution_en in info['research_institution_en']) or (self.research_institution_en in info['research_institution_en'])):
                            url_detail_page = 'https://www.aminer.cn/profile/' + self.id
                            resp_detail_page = self.downloader.crawl_data(url_detail_page, None, self.headers, "get")
                            if resp_detail_page:
                                resp_detail_page.encoding = 'utf-8'
                                content_detail_page = resp_detail_page.text

                                re_content_detail_page_list = re.findall(r"window.g_initialProps = (.+?)}};",str(content_detail_page))
                                # print(re_content_detail_page_list)
                                if re_content_detail_page_list:
                                    re_content_detail_page = re_content_detail_page_list[0]+'}}'
                                    record = self.url_detail_page(re_content_detail_page, info, url_detail_page)
                                    record['kId'] = self.kId
                                    if record:
                                        self.save_record(record, self.mongo_read_col1, {'kId': self.kId})
                                        flag_end = 1
                                        break
                        # else:
                        #     self.logger.info('专家不匹配, pass')
                        #     self.update_maintenance_table(flag_aminer=1, flag_aminer_result=0)

                        # break
                    if flag_end == 0:
                        self.logger.info('专家不匹配, pass')
                        self.update_maintenance_table(flag_aminer=1, flag_aminer_result=0)



        self.logger.info("Finish Run")




if __name__ == '__main__':

    bp = ListDetailSpider(SAVE_MONGO_CONFIG)
    bp.run(start_page=1, max_page=-1, page_size='10', round=2)
