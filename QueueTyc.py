import threading, queue
import time
import csv
import urllib.parse
from lxml import etree
import subprocess
from bs4 import BeautifulSoup


# 提取页面信息
def download_data(url, num_retries=2):
    print("消费url ", url)
    out_bytes = subprocess.check_output(['phantomjs', './code.js', url])
    out_text = out_bytes.decode('utf-8')
    dom_tree = etree.HTML(out_text)
    links = dom_tree.xpath('//div[@class="company_info_text"]/span/text()')
    #name = dom_tree.xpath('//td[@class="td-legalPersonName-value c9"]/p/a/text()')
    company_name = dom_tree.xpath('//div[@class="company_info_text"]/p/text()')
    company_business = dom_tree.xpath('//span[@ng-bind-html="perContent|splitNum"]/text()')
    # print(len(links))
    if (len(links) > 0 and len(company_name) > 0):
        print('公司名称：' + company_name[0])
        print('经营范围：'+ company_business[0])
        searchArr = ['地图','测量','测绘','地理信息','摄影','遥感']
        flag = 0
        for key,value in enumerate(searchArr):
            if company_business[0].find(value) > -1:
                flag = 1
                break
        if flag == 1:
            print("------------------------------我是分割线---------------------------------")
            csv_file = open('./company_result.csv', 'a', newline='', encoding='UTF-8')
            try:
                writer = csv.writer(csv_file)
                writer.writerow((company_name[0],url))
            except Exception as e:
                if num_retries > 0:
                    print("正在进行数据下载重试操作！！！", num_retries - 1)
                    download_data(url, num_retries - 1)
            finally:
                csv_file.close()
    else:
        if num_retries > 0:
            print("正在进行数据下载重试操作！！！",num_retries-1)
            download_data(url, num_retries-1)


def download_url(company_name,num_retries=2):
    out_bytes = subprocess.check_output(['phantomjs', './url.js',
                                         "http://www.tianyancha.com/search?key=" + urllib.parse.quote(
                                             company_name) + "&checkFrom=searchBox"])
    out_text = out_bytes.decode('utf-8')
    html = BeautifulSoup(out_text, "lxml")
    soup = html.find("a", {"class": {"query_name", "search-new-color"}})
    try:
        company_url = soup.attrs['href']
        url_queue.put(company_url)
        if url_queue.qsize()>10:
            time.sleep(5)
            print("生产者等待5s钟")
        print("生产url ", company_url)
    except Exception as e:
        print(e)
        if num_retries>0:
            print("正在进行url下载重试操作！！！", num_retries - 1)
            download_url(company_name, num_retries - 1)

    time.sleep(2)


def url_consumer(url_queue): # 提取url里面的信息
    while True:
        company_url = url_queue.get()
        download_data(company_url,2)
        time.sleep(2)
    url_queue.task_done()


def url_producer(name_queue,url_queue): # 提供url
    while True:
        company_name=name_queue.get()
        download_url(company_name,2)

# 入口
name_queue=queue.Queue() # 公司名字队列 
csv_reader = csv.reader(open('./company_before.csv', encoding="GB18030")) # 读取
for row in csv_reader:
    name_queue.put(row[0])

url_queue = queue.Queue() # 天眼查上的公司url队列
for n in range(4):
    producer_thread = threading.Thread(target=url_producer, args=(name_queue,url_queue,)) #多线程
    producer_thread.start()
for n in range(4):
    consumer_thread = threading.Thread(target=url_consumer, args=(url_queue,)) # 多线程
    consumer_thread.start()
url_queue.join()