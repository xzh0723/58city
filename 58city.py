import requests
import re
import random
from io import BytesIO
from lxml import etree
import time
import base64
import pymongo
from fontTools.ttLib import TTFont
from concurrent.futures import ProcessPoolExecutor

base_url = 'https://{city}.58.com/chuzu/pn{page}/'
#url = 'https://bj.58.com/chuzu/'

UA_LIST=[
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11',
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; Media Center PC 6.0; InfoPath.3; MS-RTC LM 8; Zune 4.7)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; Media Center PC 6.0; InfoPath.3; MS-RTC LM 8; Zune 4.7",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 4.0; InfoPath.3; MS-RTC LM 8; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 4.0; Tablet PC 2.0; InfoPath.3; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0"
    ]

headers = {
    'User-Agent': random.choice(UA_LIST)
}

PROXY_URL = 'http://127.0.0.1:5555/random'

def mongodb():
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client.city58
    return db

def get_random_proxy(proxy_url):
    try:
        response = requests.get(proxy_url)
        print('正在获取代理')
        if response.status_code == 200:
            proxy = response.text
            print('成功获取代理%s' % proxy)
            return proxy
    except Exception as e:
        print('获取代理失败', e.args)
        return None

def get_real_word(fake_word, html):
    font_ttf = re.findall("charset=utf-8;base64,(.*?)'\)", html)[0]
    #print(font_ttf)
    font = TTFont(BytesIO(base64.decodebytes(font_ttf.encode())))
    numbering = font.get('cmap').tables[0].ttFont.get('cmap').tables[0].cmap
    word_list = []
    for char in fake_word:
        decode_num = ord(char)
        if decode_num in numbering:
            num = numbering[decode_num]
            num = int(num[-2:]) - 1
            word_list.append(num)
        else:
            word_list.append(char)
    real_word = ''
    for num in word_list:
        real_word += str(num)
    return real_word

def get_index(url, options={}):
    print('正在抓取' + url)
    #proxies = dict({'proxies': ''}, **options)
    response = requests.get(url, headers=headers)
    print(response)
    if response.status_code == 200:
        return response
    else:
        print('抓取失败')

def parse_list(url):
    db = mongodb()
    html = get_index(url).text
    #print(html)
    item = {}
    page = etree.HTML(html)
    li = page.xpath('.//ul[@class="listUl"]//li')[0:-1]
    #print(li)
    for each_li in li:
        title = each_li.xpath('.//div[@class="des"]/h2/a/text()')[0].strip()
        item['title'] = get_real_word(title, html)
        item['src'] = each_li.xpath('.//div[@class="des"]/h2/a/@href')[0]
        price = each_li.xpath('.//div[@class="money"]/b/text()')[0]
        item['price'] = get_real_word(price, html)
        geju = each_li.xpath('.//div[@class="des"]/p/text()')[0]
        item['room'] = get_real_word(geju, html)
        item['address'] = '/'.join(each_li.xpath('.//p[@class="add"]/a/text()'))
        if each_li.xpath('.//span[@class="listjjr"]/text()'):
            jjr = each_li.xpath('.//span[@class="listjjr"]/text()')[0].strip()
        else:
            jjr = 'None'
        item['jjr'] = jjr
        print(item)
        db.zufang.insert(dict(item))
        time.sleep(random.random())
        if not 'https:' in item['src']:
            url = 'https:' + item['src'] + '?reform=pcfront'
        else:
            url = item['src']
        res_1 = get_index(url)
        try:
            parse_detail(res_1.text)
        except:
            print('爬取过于频繁，出现验证码')

def parse_detail(html):
        db = mongodb()
        item = {}
        page = etree.HTML(html)
        price = page.xpath('//b[contains(@class, "f36")]/text()')
        #if price:
        item['price'] = get_real_word(price[0], html)
        item['pay_method'] = page.xpath('//div[contains(@class, "f16")]/span[2]/text()')[0]
        item['rental_mathod'] = page.xpath('//ul[@class="f14"]/li[1]/span[2]/text()')[0]
        house = page.xpath('//ul[@class="f14"]/li[2]/span[2]/text()')[0]
        house = get_real_word(house, html)
        item['house'] = {
            'type': house.split(' ')[0],
            'area': re.findall('卫.*?(\d+).*?平', house, re.S)[0],
            'decoration': house.split(' ')[-1]
        }
        geju = page.xpath('//ul[@class="f14"]/li[3]/span[2]/text()')[0]
        item['geju'] = get_real_word(geju, html)
        item['xiaoqu'] = page.xpath('//ul[@class="f14"]/li[4]/span[2]/a/text()')[0]
        item['rigion'] = ''.join(page.xpath('//ul[@class="f14"]/li[5]/span[2]//text()')).replace(' ', '').strip()
        traffic = page.xpath('//ul[@class="f14"]/li[5]/em/text()')
        if traffic:
            item['traffic'] = traffic[0]
        item['address'] = page.xpath('//ul[@class="f14"]/li[6]/span[2]/text()')[0].strip()
        item['tags'] = '/'.join(page.xpath('//ul[@class="introduce-item"]/li[1]/span[2]/em/text()'))
        item['descrition'] = ' '.join(page.xpath('//ul[@class="introduce-item"]/li[2]/span[2]//text()'))
        imgs = page.xpath('//ul[@id="housePicList"]/li/img/@lazy_src')
        image = []
        for img in imgs:
            image.append(img)
            item['image'] = image
        print(item)
        db.zufang_detail.insert(dict(item))
        #return item

if __name__ == '__main__':
    city = input('请输入您要查询的城市（城市名称的首字母小写缩写）: ')
    #area = input('请输入您要查询的区域：')
    #retal = input('请输入您意向的租赁方式：')
    #bedroom = input('请输入您的卧室选择: ')
    #oriented = input('请输入您期望的朝向：')
    #decorate = input('请输入您期望的装修情况：')
    max_page_num = input('请输入您要查询的页数：')
    #lowest_price = input('请输入您期望的最低价格：')
    #highest_price = input('请输入您期望的最高价格：')
    start = time.time()
    pool = ProcessPoolExecutor()
    for page in range(1, int(max_page_num) + 1):
        url = base_url.format(city=city, page=page)
        pool.submit(parse_list, url)

    print('耗时：', time.time() - start)
