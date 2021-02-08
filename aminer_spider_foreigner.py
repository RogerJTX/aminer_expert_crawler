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
from etl.data_gather.settings import SAVE_MONGO_CONFIG2, RESOURCE_DIR
from etl.common_spider.donwloader import Downloader
import json



class ListDetailSpider(object):
    def __init__(self, config, proj=None):
        config["db"] = 'res_kb'
        self.proj = proj
        self.host = "www.aminer.cn"  # 网站域名
        self.host2 = "www.aminer.cn"
        self.host_name = "Aminer"  # 网站中文名
        self.api_url = "https://www.aminer.cn/profile/"  # 起始URL或者是基础URL，请求的链接在此基础生成
        self.mongo_client = self.get_mongo(**config)
        self.save_coll_name = "res_kb_expert_aminer_car"  # 需要保存的表名
        self.mongo_db = self.mongo_client[config["db"]]
        self.mongo_coll = self.mongo_db[self.save_coll_name]

        config["db1"] = 'res_kb'
        self.read_col1_name = "res_kb_expert_article_car"
        self.mongo_read_db1 = self.mongo_client[config["db1"]]
        self.mongo_read_col1 = self.mongo_read_db1[self.read_col1_name]

        config["db2"] = 'res_kb'
        self.read_col2_name = "res_kb_expert_relation_car"
        self.mongo_read_db2 = self.mongo_client[config["db2"]]
        self.mongo_read_col2 = self.mongo_read_db2[self.read_col2_name]

        config["db3"] = 'res_kb'
        self.read_col3_name = "industry_keyword_expert_name"
        self.mongo_read_db3 = self.mongo_client[config["db3"]]
        self.mongo_read_col3 = self.mongo_read_db3[self.read_col3_name]

        config["db4"] = 'res_kb'
        self.read_col4_name = "aminer_maintenance_table_keywords"
        self.mongo_read_db4 = self.mongo_client[config["db4"]]
        self.mongo_read_col4 = self.mongo_read_db4[self.read_col4_name]

        self.start_down_time = datetime.datetime.now()
        self.down_retry = 5
        configure_logging("AMINER_R3.log")  # 日志文件名
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
                    page_num = (num_total / page_size) + 1
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


        project = []
        url_project = 'https://apiv2.aminer.cn/magic?a=GetFundsByPersonID__person.GetFundsByPersonID__'
        data = '[{"action":"person.GetFundsByPersonID","parameters":{"id":"'+self.id+'","end":100,"start":0}}]'
        resp_project = self.downloader.crawl_data(url_project, data, self.headers, "post")
        if resp_project:
            resp_project.encoding = 'utf-8'
            content_project = resp_project.text
            json_content = json.loads(content_project)
            data = json_content['data']
            if 'items' in data[0]:
                items = data[0]['items']
                items_list = items
                for t in items_list:
                    record_i_t = {}
                    record_i_t['project_name_zh'] = t['title_zh']
                    record_i_t['project_name_en'] = t['title']
                    if 'url' in t:
                        record_i_t['project_url'] = t['url']
                    else:
                        record_i_t['project_url'] = ''
                    if 'register_number' in t:
                        record_i_t['project_number'] = t['register_number']
                    else:
                        record_i_t['project_number'] = ''
                    record_i_t['project_amount'] = t['amount']
                    record_i_t['currency'] = t['currency']
                    record_i_t['abstract_en'] = t['abstract']
                    record_i_t['abstract_zh'] = t['abstract_zh']
                    if 'keywords' in t:
                        record_i_t['keywords_en'] = t['keywords']
                    else:
                        record_i_t['keywords_en'] = []
                    if 'keywords_zh' in t:
                        record_i_t['keywords_zh'] = t['keywords_zh']
                    else:
                        record_i_t['keywords_zh'] = []
                    if t['start_year'] != t['end_year']:
                        record_i_t['project_time'] = str(t['start_year'])+'-'+str(t['end_year'])
                    else:
                        record_i_t['project_time'] = str(t['start_year'])
                    project.append(record_i_t)
        else:
            self.logger.info('Project请求未响应')


        record['project'] = project
        record['source'] = '专家详情页'

        return record

    # 更新维护表
    # def update_maintenance_table(self, record_in, round):
    #     record = {}
    #     record['expert_name_en'] = record_in['contactor_name_en']
    #     record['expert_name_zh'] = record_in['contactor_name_zh']
    #     record['category'] = ''
    #     record['round'] = round + 1
    #     record['update_time'] = datetime.datetime.now()
    #     record['add_time'] = datetime.datetime.now()
    #     record['id'] = record_in['contactor_id']
    #     record['url'] = 'https://www.aminer.cn/profile/' + record_in['contactor_id']
    #     r_in_db = self.mongo_read_col4.find_one({'id':record['id'], 'expert_name_en':record['expert_name_en'], 'expert_name_zh':record['expert_name_zh']})
    #     if not r_in_db:
    #         self.mongo_read_col4.insert_one(record)
    #         self.logger.info('维护表成功插入专家, id:%s' % record['id'])
    #     else:
    #         self.logger.info('维护表重复专家, 跳过, id:%s' % record['id'])


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

        page_size = 50
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
        for each in self.mongo_read_col4.find():
            # print(each)
            dict_mongo = {}
            search_key = each['keyword']
            category = each['category']
            round_judge = each['round']

            if round_judge == round:
                    dict_mongo['search_key'] = search_key
                    dict_mongo['category'] = category
                    dict_mongo['round'] = round
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
            flag_ccc = 0
            self.logger.info(each_list_mongo)
            c_number += 1
            self.logger.info('第几个搜索关键词：%s' % str(c_number))

            search_key = each_list_mongo['search_key']
            category = each_list_mongo['category']
            # 查询部分
            if search_key == "IGBT":
                flag = 1

            if flag == 1:
                time.sleep(5)
                self.expert_name = search_key

                nation_list = ['USA', 'Japan', 'United Kingdom', 'Germany', 'Netherlands', 'Sweden', 'Australia', 'Italy', 'Switzerlan', 'Greece', 'Russia']
                for nation_search in nation_list:
                    # 列表页
                    list_data = []
                    url_list_page = 'https://apiv2.aminer.cn/n?a=__searchapi.SearchPerson___'
                    url_list_page_post_data = '[{"action":"searchapi.SearchPerson","parameters":{"offset":0,"size":100,"query":"'+self.expert_name+'","include":["agg","intelli","topics"],"as_nationality":"'+nation_search+'","aggregation":["gender","h_index","nation","lang"]},"schema":{"person":["id","name","name_zh","avatar","tags","is_follow","num_view","num_follow","is_upvoted","num_upvoted","is_downvoted","bind",{"profile":["position","position_zh","affiliation","affiliation_zh","org"]},{"indices":["hindex","gindex","pubs","citations","newStar","risingStar","activity","diversity","sociability"]},"tags_translated_zh"]}}]'
                    # print(url_list_page_post_data)
                    resp_list_page = self.downloader.crawl_data(url_list_page, url_list_page_post_data.encode("utf-8").decode("latin1"), self.headers, "post")
                    if resp_list_page:
                        resp_list_page.encoding = 'utf-8'
                        content_list_page = resp_list_page.text
                        list_data, num_page_total = self.url_list_page(content_list_page, page_size)

                    else:
                        self.logger.info('列表页不响应')

                    if list_data:
                        for result_e_num, info in enumerate(list_data):

                            self.logger.info('搜索列表结果第几个专家:%s' % result_e_num)
                            for num_round in range(1,4):
                                time.sleep(3)
                                # 基本信息
                                if num_round == 1:
                                    id = info['id']
                                    self.expert_name_zh = info['expert_name_zh']
                                    self.expert_name_en = info['expert_name_en']
                                    self.id = id
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
                                            if record:
                                                # print(record)
                                                if 'pubs' not in record['indices']:
                                                    self.logger.info('专家数据论文数小于3, 跳过')
                                                    flag_ccc = 1
                                                    break
                                                else:
                                                    num_paper = record['indices']['pubs']
                                                    if int(num_paper) > 2:
                                                        self.save_record(record, self.mongo_coll, {'id': self.id})
                                                    else:
                                                        self.logger.info('专家数据论文数小于3, 跳过')
                                                        break

                                    time.sleep(3)
                                # 论文
                                elif num_round == 2:
                                    page_num = 1
                                    round_article = 0
                                    while page_num > 0:

                                        url_article = 'https://apiv2.aminer.cn/magic?a=GetPersonPubs__person.GetPersonPubs___'
                                        data_article = '[{"action":"person.GetPersonPubs","parameters":{"offset":'+str(round_article*500)+',"size":500,"sorts":["!year"],"ids":["'+self.id+'"],"searchType":"all"},"schema":{"publication":["id","year","title","title_zh","authors._id","authors.name","authors.name_zh","num_citation","venue.info.name","venue.volume","venue.info.name_zh","venue.issue","pages.start","pages.end","lang","pdf","doi","urls","versions"]}}]'
                                        resp_article_page = self.downloader.crawl_data(url_article, data_article, self.headers, "post")
                                        if resp_article_page:
                                            resp_article_page.encoding = 'utf-8'
                                            content_article_page = resp_article_page.text
                                            list_record_article, page_num = self.url_article(content_article_page)
                                            if list_record_article:
                                                for each_article in list_record_article:
                                                    # print(each_article)
                                                    self.save_record(each_article, self.mongo_read_col1, {'title':each_article['title'],
                                                                                     'expert_name_zh':each_article['expert_name_zh'],
                                                                                     'expert_name_en':each_article['expert_name_en'],
                                                                                    'authors':each_article['authors']})
                                        round_article += 1
                                        page_num -= 1
                                        time.sleep(1)
                                    time.sleep(3)
                                # 关系图
                                elif num_round == 3:
                                    url_graph = 'https://apiv2.aminer.cn/n?a=GetEgoNetworkGraph__personapi.GetEgoNetworkGraph___'
                                    data_graph = '[{"action":"personapi.GetEgoNetworkGraph","parameters":{"id":"'+self.id+'","reloadcache":true}}]'
                                    resp_graph_page = self.downloader.crawl_data(url_graph, data_graph, self.headers, "post")
                                    if resp_graph_page:
                                        resp_graph_page.encoding = 'utf-8'
                                        content_graph = resp_graph_page.text
                                        list_record_graph = self.url_graph(content_graph)
                                        if list_record_graph:
                                            for each_graph in list_record_graph:
                                                # print(each_graph)

                                                # 更新维护表
                                                # self.update_maintenance_table(each_graph, round)

                                                self.save_record(each_graph, self.mongo_read_col2,
                                                                 {'contactor_id': each_graph['contactor_id'],
                                                                  'expert_name_zh': each_graph['expert_name_zh'],
                                                                  'expert_name_en': each_graph['expert_name_en'],
                                                                  'contactor_name_zh':each_graph['contactor_name_zh'],
                                                                  'contactor_name_en':each_graph['contactor_name_en'],
                                                                  'id': each_graph['id']})
                                    else:
                                        self.logger.info('关系图请求未响应')
                                    time.sleep(3)

                                if flag_ccc == 1:
                                    self.logger.info('跳过此列表专家, 进入下一个名称搜索')
                                    break
                            if flag_ccc == 1:
                                self.logger.info('跳过此列表专家, 进入下一个名称搜索')
                                break

        self.logger.info("Finish Run")




if __name__ == '__main__':

    bp = ListDetailSpider(SAVE_MONGO_CONFIG2)
    bp.run(start_page=1, max_page=-1, page_size='100', round=1)
