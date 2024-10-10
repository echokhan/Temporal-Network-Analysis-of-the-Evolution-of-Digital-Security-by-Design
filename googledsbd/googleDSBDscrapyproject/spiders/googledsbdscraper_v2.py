import scrapy
from urllib.parse import urlencode, urlparse
import time
from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup

class GoogledsbdscraperSpider_v2(scrapy.Spider):
    name = "googledsbdscraper_v2"
    allowed_domains = ["www.google.com", "google.com"]
    start_urls = ["https://www.google.com"]
    #search_keyword = "\"digital security by design\""
    #search_keyword = "\"dsbd\""
    domain_list = ['.uk', '.eu']
    #version_dict = {"\"digital security by design\"": 0, "\"dsbd\"": 1, "\"Digital Security\"": 2}
    #search_keyword = list(version_dict.keys())[2]
    #version_tag = version_dict[search_keyword]
    #version_tag = 'v2' if search_keyword == "\"dsbd\"" else 'v1'
    search_keyword_list = ['\"digital security by design\"', '\"dsbd\"', '\"Digital Security\"']

    domain_tag = domain_list[0].replace('.', '')
    
    show_date_keyword = "&tbs=cdr:1,cd_min:1/1/0" #used to show date as per google, of created or last updated date of google search results
    custom_settings = {
        'ROBOTSTXT_OBEY':'False',
        'FEEDS': {
            f'./scrapingoutput/{name}.json': {
                'format': 'json',
                'overwrite': True,
            },
            f'./scrapingoutput/{name}.csv': {
                'format': 'csv',
                'overwrite': True,
            }
        }
    }

    def _get_page_source(self, link: str):
        time.sleep(8)
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)
        driver.get(link)
        # refresh to remove popup window ---------
        time.sleep(8)
        driver.refresh()
        time.sleep(8)
        html = driver.page_source
        driver.quit()
        return html

    def _create_google_url(self, query):
        # Create a dictionary with the search query and the number of results
        google_dict = {'q': query, 'num': 300, }
        
        # Return the complete Google search URL without the site restriction
        return 'https://www.google.com/search?' + urlencode(google_dict)
    
    def _get_google_url(self):
        url_list = []
        for domain in self.domain_list:
            for search_keyword in self.search_keyword_list:
                query = f"{search_keyword} site:{domain}"
                print('--------------------------------------------------------------')
                result_url = self._create_google_url(query) + self.show_date_keyword
                print(result_url)
                url_list.append((search_keyword, domain, result_url))
                print('--------------------------------------------------------------')
        return url_list

    #def start_requests(self, response):
    #    url_list = self._get_google_url()
    #    for url in url_list:
    #        page_source = self._get_page_source(url)
    #        print(type(page_source))
    #        yield scrapy.Request(url=None, callback=parse, cb_kwargs=dict(url=url, page_source=page_source))

    def parse(self, response):
        url_list = self._get_google_url()
        #url = url_list[0]
        index = 0
        for url_tuple in url_list:
            keyword = url_tuple[0]
            domain = url_tuple[1]
            url = url_tuple[2]

            page_source = self._get_page_source(url)
            print('************************************')
            print('We are in parse function')
            print(page_source)
            print('************************************')

            # Create a Scrapy response object from the Selenium page source
            selenium_response = HtmlResponse(url=url, body=page_source, encoding='utf-8')

            # Now you can use Scrapy selectors on the selenium_response
            entries = selenium_response.css('div.MjjYud')
            len_entries = len(entries)
            print(f"A total of {len_entries} urls found")
            for i, entry in enumerate(entries):
                index +=1
                print(f'index: {index}')
                print('----------------------------')
                #print(entry.css('a.heading::text').get().strip())
                result_title = entry.css('h3.LC20lb.MBeuO.DKV0Md::text').get()
                if result_title is not None:
                    result_title = result_title.strip()
                    print(f'title: {result_title}')
                print('----')
                result_url = entry.css('a[jsname="UWckNb"]::attr(href)').get()
                if result_url is not None:
                    result_url = result_url.strip()
                    print(f'url: {result_url}')

                print('----')
                result_date = entry.css('div > div > div:nth-child(2) > div > span:first-child > span::text').get()
                if result_date is not None:
                    result_date = result_date.strip()
            
                print(f"date: {result_date}")

                print('----')
                #result_page_source = self._get_page_source(result_url)
                #soup = BeautifulSoup(page_source, 'lxml')
                #result_text = soup.get_text().strip()
                #print(f'text: {result_text}')
                # Extract all text from the webpage
            
                yield {
                'index': index,
                'searched_keyword': keyword,
                'site_domain': domain,
                'result_title': result_title,
                'result_date': result_date,
                'result_url': result_url
                }