from scrapy.utils.project import get_project_settings
from scrapy.spiderloader import SpiderLoader
from scrapy.crawler import CrawlerProcess

settings = get_project_settings()
sl = SpiderLoader.from_settings(settings)
sl.list()
for spider_name in sl.list():
    spider = sl.load(spider_name)
    #print(spider_name)
    try:
        print(spider.data_type)
    except:
        print('XXXXXXXXXXXXXXXXXx')





#runner = CrawlerRunner(sl.load('zambia'),settings)

from testfixtures import LogCapture

with LogCapture() as l:
    runner = CrawlerProcess(settings)
    runner.crawl('zambia')
    runner.start()

import pdb; pdb.set_trace()

#deferred = runner.join()






