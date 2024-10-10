import json
import re
import scrapy
import requests
import fitz  # PyMuPDF
from io import BytesIO



class ResultsdsbdscraperSpider(scrapy.Spider):
    name = "resultsdsbdscraper"
    #allowed_domains = ["google.com"]
    #start_urls = ["https://google.com"]
    custom_settings = {
        'ROBOTSTXT_OBEY':'False',
        'FEEDS': {
            f'./scrapingoutput/{name}_v2.json': {
                'format': 'json',
                'overwrite': True,
            },
            f'./scrapingoutput/{name}_v2.csv': {
                'format': 'csv',
                'overwrite': True,
            }
        }
    }

    def read_json_file(self, file_path, encoding='utf-8'):
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                return json.load(f)
        except UnicodeDecodeError:
            # If UTF-8 fails, try with UTF-8-sig (to handle BOM)
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                return json.load(f)

    def _extract_pdf(self, url):
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Ensure the request was successful

        # Load the PDF content into PyMuPDF
        pdf_content = BytesIO(response.content)
        document = fitz.open(stream=pdf_content, filetype="pdf")

        # Extract text from each page
        text = ""
        for page_num in range(len(document)):
            page = document.load_page(page_num)
            text += page.get_text()

        return text.replace('\n', ' ').replace('\t', ' ')
    
    def start_requests(self):
        #with open ('.\scrapingoutput\googledsbdscraper_v2.json', "r") as f:
        #    data = json.loads(f.read())
        data = self.read_json_file('.\scrapingoutput\googledsbdscraper_v2.json')
        
        print(f'Length of scraped result list: {len(data)}')
        for metadata_dict in data:
            result_date = metadata_dict['result_date']
            result_url = metadata_dict['result_url']
            result_title = metadata_dict['result_title']
            searched_keyword = metadata_dict['searched_keyword']
            site_domain = metadata_dict['site_domain']

            if result_url is not None:
                print(f'Scraping text from url: {result_url}')
                yield scrapy.Request(url=result_url, callback=self.parse, cb_kwargs={'result_title': result_title, 'result_date': result_date, 'result_url': result_url, 'site_domain': site_domain, 'searched_keyword': searched_keyword})


    def parse(self, response, result_title, result_date, result_url, site_domain, searched_keyword):
        #extract text, removing lines with only nextlines/tabs and removing multiple tabs from text
        if '.pdf' in response.url:
            #print(f'Cannot for now scrape text from pdf from: {response.url}')
            result_text = self._extract_pdf(response.url)
            yield {
                'searched_keyword': searched_keyword,
                'site_domain': site_domain,
                'result_title': result_title,
                'result_date': result_date,
                'result_url': result_url,
                'result_text': result_text
            }
        else:
            if response.status == 200:
                text_list = [re.sub(r' +', ' ', re.sub(r'\t+', '\t', i)).replace('\n', ' ') for i in response.xpath("//body//text()").extract() if bool(re.search(r'[^\n\t]', i))]
                result_text = ' '.join(text_list).strip()
            else:
                result_text = f'****Got response status: {response.status}. No text returned.****'

            yield {
                'searched_keyword': searched_keyword,
                'site_domain': site_domain,
                'result_title': result_title,
                'result_date': result_date,
                'result_url': result_url,
                'result_text': result_text
            }