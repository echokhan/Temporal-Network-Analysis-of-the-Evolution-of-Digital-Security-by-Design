import re
import json
import csv
import time
from datetime import timedelta
import pandas as pd

from googleDSBDscrapyproject.spiders.googledsbdscraper_v2 import GoogledsbdscraperSpider_v2
from googleDSBDscrapyproject.spiders.resultsdsbdscraper import ResultsdsbdscraperSpider

from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
from twisted.internet import reactor, defer


def main():
    #################################################
    ###########Running spiders sequentially##########
    #################################################
    print("this is the main.py")
    settings = get_project_settings()
    configure_logging(settings)
    runner = CrawlerRunner(settings)


    @defer.inlineCallbacks
    def crawl():
        print("Running crawler GoogledsbdscraperSpider_v2")
        start_time = time.time()  # Record start time
        yield runner.crawl(GoogledsbdscraperSpider_v2)
        elapsed_time_googledsbdscraper = time.time() - start_time
        print(f"GoogledsbdscraperSpider_v2 finished. Elapsed time: {str(timedelta(seconds=elapsed_time_googledsbdscraper))}")

        print("Running crawler ResultsdsbdscraperSpider")
        start_time = time.time()  # Record start time
        yield runner.crawl(ResultsdsbdscraperSpider)
        elapsed_time_resultsdsbdscraper = time.time() - start_time
        print(f"ResultsdsbdscraperSpider finished. Elapsed time: {str(timedelta(seconds=elapsed_time_resultsdsbdscraper))}")

        reactor.stop()


    crawl()
    reactor.run()

    ################################
    #######Merging data#############
    ################################
    #We have scraped google search results separately (first spider)
    #and scraped text from each search result url separately (second spider)
    #Now merging data

    #####################################################
    #######Read google research results json#############
    #####################################################
    def read_json_file(file_path, encoding='utf-8'):
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                return json.load(f)
        except UnicodeDecodeError:
            # If UTF-8 fails, try with UTF-8-sig (to handle BOM)
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                return json.load(f)
    #with open ('./scrapingoutput/googledsbdscraper_v2.json', "r") as f:
    #    google_results = json.loads(f.read())
    google_results = read_json_file('./scrapingoutput/googledsbdscraper_v2.json')

    df_google_results = pd.DataFrame(google_results)
    df_google_results.set_index('index', inplace=True)
    print(df_google_results.tail(10))
    print(f"df_google_results Length: {len(df_google_results)}")
    
    #####################################################
    #######Read research results text csv###############
    #####################################################
    df_search_results = pd.read_csv('./scrapingoutput/resultsdsbdscraper_v2.csv')
    df_search_results['result_text'] = df_search_results['result_text'].str.replace('\t', ' ')
    df_search_results['result_text'] = df_search_results['result_text'].str.replace('\r', ' ')
    df_search_results['result_text'] = df_search_results['result_text'].str.replace('\n', ' ')
    df_search_results['result_text'] = df_search_results['result_text'].str.replace('{', '')
    df_search_results['result_text'] = df_search_results['result_text'].str.replace('}', '')
    df_search_results['result_text'] = df_search_results['result_text'].str.replace('(', '')
    df_search_results['result_text'] = df_search_results['result_text'].str.replace(')', '')
    df_search_results['result_text'] = df_search_results['result_text'].str.replace('"', '')

    print(df_search_results.tail(10))
    print(f"df_search_results Length: {len(df_search_results)}")



    #####################################################
    #######Merge dfs and save result as csv##############
    #####################################################
    df_merged = pd.merge(df_google_results, df_search_results, on=['searched_keyword', 'site_domain', 'result_date', 'result_url'])[['searched_keyword', 'site_domain', 'result_title_x', 'result_date', 'result_url', 'result_text']]
    df_merged.rename(columns={'result_title_x': 'result_title'}, inplace=True)
    print(df_merged.tail(10))
    print(f"df_merged Length: {len(df_merged)}")

    print("Keyword Distribution:")
    print(df_merged['searched_keyword'].value_counts())
    df_merged.to_csv('./scrapingoutput/merged_results.csv', index=True)


if __name__ == '__main__':
    main()