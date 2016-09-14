#coding=utf-8
from datetime import datetime
import logging

def urls_status_4_parse(data,conn,cur):
    # data 是一个字典的类型
    # URL_STATUS 第四种类型
    sql=u"INSERT INTO urls_status(create_time,update_time,source_link_id,retry_count,is_retry,url_type) " \
        u"VALUES(%s,%s,%s,%s,%s,%s)"
    try:
        cur.execute(sql,(datetime.now(),datetime.now(),
                         data['source_link_id'],data['retry_count'],
                         data['is_retry'],data['url_type']))
        conn.commit()
        logging.info('urls_status_4_parse Successful : %s'%data['source_link_id'])
        return True
    except Exception as e:
        conn.rollback()
        logging.error('urls_status_4_parse insert Error : %s  %s'%(data['source_link_id'],e))
        return False



def urls_status_3_parse(data,conn,cur):
    # data 是一个字典的类型
    # URL_STATUS 第3种类型 重试
    # 两种情况  一种是新的  一种是旧的
    if data['retry_count']>1:# 旧的
        sql=u"update urls_status set update_time=%s,retry_count=%s,is_retry=%s where source_link_id=%s"
        insert_data=(datetime.now(),data['retry_count'],data['is_retry'],data['source_link_id'])
    else: # 新的
        sql=u"INSERT INTO urls_status(create_time,update_time,source_link_id,retry_count,is_retry,url_type) " \
            u"VALUES(%s,%s,%s,%s,%s,%s)"
        insert_data=(datetime.now(),datetime.now(),
                         data['source_link_id'],data['retry_count'],
                         data['is_retry'],data['url_type'])
    try:
        cur.execute(sql,insert_data)
        conn.commit()
        logging.info('urls_status_3_parse Successful : %s'%data['source_link_id'])
        return True
    except Exception as e:
        conn.rollback()
        logging.error('urls_status_3_parse insert Error : %s  %s'%(data['source_link_id'],e))
        return False

def urls_status_1_parse(data,conn,cur):
    # data 是一个字典的类型
    # URL_STATUS 第1种类型 重试

    # 两种情况 一种是首次就成功的  宁外就是重试成功的

    if data['retry_count']>1:# 旧的
        sql=u"update urls_status set update_time=%s,retry_count=%s,is_retry=%s where source_link_id=%s"
        insert_data=[
            datetime.now(),data['retry_count'],data['is_retry'],data['source_link_id']
        ]
    else: # 新的
        sql=u"INSERT INTO urls_status(create_time,update_time,source_link_id,retry_count,is_retry,url_type) " \
            u"VALUES(%s,%s,%s,%s,%s,%s)"
        insert_data=[datetime.now(),datetime.now(),
                         data['source_link_id'],data['retry_count'],
                         data['is_retry'],data['url_type']
                     ]


    sql_company_survey=u"INSERT INTO company_survey_fix(" \
    u"create_time,update_time,title,source_link_id,source_link,stock_name,stock_num,date_time," \
    u"paper_column,paper_type,researcher,institution,page_sum," \
    u"attention_rate,attachment_size,provider,abstract) " \
    u"VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    insert_data_company_survey=(datetime.now(),datetime.now(),data['title'],data['source_link_id'],data['source_url'],data['stock_name'],
                                data['stock_num'],data['share_time'],data['paper_column'],
                 data['paper_type'],data['researcher'],data['institution'],data['page_sum'],data['attention_rate'],data['attachment_size'],
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




## 这是一个重要的函数 检验Url 的状态
def checkout_url_status(page,cur):
    ## 传入url的page 返回对应的结果
    """
    首先我们要查询是否需要重试
    不需要就直接返回False
    需要重试的话 那我们就返回
    source_link_id
    retry_count
    is_retry
    url_type
    这些信息
    """
    sql="SELECT retry_count,is_retry,url_type FROM urls_status WHERE source_link_id=%s"
    cur.execute(sql,(page,))
    result_data=cur.fetchone()
    if result_data:
        if result_data[1]:#是否重试  1 为重试  0为不重试
            retry_count,is_retry,url_type=result_data
            return (retry_count+1,is_retry,url_type,page)
        else:
            return (False,)*4
    else:
        return (1,1,2,page)  #自己去看配置文件的urls_status

