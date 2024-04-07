import re
import scrapy
from bs4 import BeautifulSoup
from scrapy.linkextractors import LinkExtractor
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from w3lib.url import url_query_cleaner
import extruct
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings

import warnings
warnings.filterwarnings("ignore", category=scrapy.exceptions.ScrapyDeprecationWarning)


class DownfilesItem(scrapy.Item):
    file_urls = scrapy.Field()
    file_size = scrapy.Field()
    content_type=scrapy.Field()
   # files = scrapy.Field
def process_links(links):
    for link in links:
        link.url = url_query_cleaner(link.url)
        yield link


class NYTimesCrawler(CrawlSpider):

    name = 'nytimes'
    handle_httpstatus_list = [200, 301, 401, 403, 404, 429, 404, 302, 520]

    allowed_domains = ['www.nytimes.com']
    start_urls = ["https://www.nytimes.com"]
    rules = (
        Rule(
            LxmlLinkExtractor(
                tags=('img', 'a', 'area', 'link', 'script'),
                attrs=('src', 'href'),
                deny=[
                    re.escape('https://www.nytimes.com/offsite'),
                    re.escape('https://www.nytimes.com/whitelist-offsite'),
                ],
            ),
            process_links=process_links,
            callback='parse_item',
            follow=True
        ),
    )

    def parse_item(self, response):
        with open('filefile1.csv', 'a') as f1:
            f1.write("%s %s\n" % (response.url,response.status))
        f1.close()
        file_extension = response.url.split('.')[-1]

        if file_extension.lower() in ('zip','html', 'pdf', 'doc','png','jpg','gif','jpeg'):
            page = response.url.split('/')[-1]
            filename = '/home/ritu/PycharmProjects/pythonProject5/scrapy_crawler/scrapy_crawler/spiders/downloads/%s' % page
            with open(filename, 'wb') as f:
                f.write(response.body)

            file_name = str(page)
            import os
            if os.path.isfile(('/home/ritu/PycharmProjects/pythonProject5/scrapy_crawler/scrapy_crawler/spiders/downloads/' + str(file_name))):
                file_size = os.path.getsize('/home/ritu/PycharmProjects/pythonProject5/scrapy_crawler/scrapy_crawler/spiders/downloads/' + str(file_name))

                file_url = response.css('.downloadline::attr(href)').get()
                file_url = response.urljoin(file_url)
                from urllib.request import urlopen
                from urllib.request import Request, urlopen
                req = Request(
                    url=file_url,
                    headers={
                        'User-Agent': "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36"}
                )
                webpage = urlopen(req).read()
                soup = BeautifulSoup(webpage, features="lxml")
                links = soup.find_all('a')
                links_outbound = []
                links_inbound=[]
                links_outbound_set = set()
                links_inbound_set = set()
                for link in links:
                    l = link.get('href')
                    if "https://www.nytimes.com" not in l:
                        links_outbound += [link.get('href')]
                        links_outbound_set.add(link.get('href'))
                    else:
                        links_inbound+=[link.get('href')]
                        links_inbound_set.add(link.get('href'))



                return {

                    'url': response.url,
                    'file_size': file_size,
                    'links_inbound':len(links_inbound),
                    'links_outbound':len(links_outbound),
                    'links_inbound_set':len(list(links_inbound)),
                    'links_outbound_set':len(list(links_outbound)),
                    'content_type': response.headers['Content-Type'][:-1],

                }
