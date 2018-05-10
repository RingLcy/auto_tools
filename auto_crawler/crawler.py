# -*- coding: utf-8 -*-
# filename: crawler.py

import sqlite3  
import urllib2  
import os, sys
import multiprocessing
from HTMLParser import HTMLParser  
from urlparse import urlparse
from hash_helper import HashHelper
from optparse import OptionParser
from logging import *

thread_num_multiple = 1

class MyHTMLParser(HTMLParser):  
    """
    Parser that extracts hrefs
    """
    def __init__(self):
        HTMLParser.__init__(self)
        
    
    def handle_starttag(self, tag, attrs):        
        #print tag, attrs
        if tag in ('iframe', 'script'):
            dict_attrs = dict(attrs)
            if dict_attrs.get('src') and '.js' in dict_attrs['src']:
                    self.js_links.add(dict_attrs['src'])
    
    def _get_full_url(self, scheme, domain, path):
        u_parse = urlparse(path)
        if not u_parse.scheme:
            if path.startswith('/'):
                result = scheme + "://" + domain + u_parse.path
            else:
                result = scheme + "://" + domain + '/' + u_parse.path
        else:
            result = path
        return result

    def get_js_links(self, html, referer_url):
        result = set()
        u_parse = urlparse(referer_url)

        self.js_links = set()
        self.feed(html)
        for js_link in self.js_links:
            js_link = self._get_full_url(u_parse.scheme, u_parse.netloc, js_link)
            result.add(js_link)
        return result


class Crawler(object):  
    def __init__(self, depth=1, crawl_js=False):
        """
        depth: how many time it will bounce from page one (optional)
        cache: a basic cache controller (optional)
        """
        self.depth = depth
        self.content = {}
        self.html_parser = MyHTMLParser()
        self.crawl_js = crawl_js

    def _set(self, url, html):
        self.content[url] = html

    def _get(self, url):
        page = self._curl(url)
        if page:
            print "cached url... %s" % (url)
        return page

    def _curl(self, url):
        """
        return content at url.
        return empty string if response raise an HTTPError (not found, 500...)
        """
        try:
            print "retrieving url... %s" % (url)
            req = urllib2.Request(url)
            req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36')
            response = urllib2.urlopen(req)
            return response.read().decode('ascii', 'ignore')
        except Exception, e:
            print "error %s: %s" % (url, e)
            return ''

    def _get_inner_links(self, html, url):
        if self.crawl_js:
            return self.html_parser.get_js_links(html, url)
        return set()

    def _crawl(self, urls, max_depth):
        n_urls = set()
        if max_depth:
            for url in urls:
                # do not crawl twice the same page
                if url not in self.content:
                    html = self._get(url)
                    self._set(url, html)
                    if max_depth > 1:
                        n_urls = n_urls.union(self._get_inner_links(html, url))
                    #print n_urls
            self._crawl(n_urls, max_depth-1)
    
    def _save_crawl_content(self, out_dir):
        hash_helper = HashHelper()
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        result = {}
        for url in self.content:
            content = self.content[url]
            sha1 = hash_helper.calc_sha1(content)
            file_path = os.path.join(out_dir, sha1+'.html')
            if not os.path.exists(file_path) and len(content):
                with open(file_path, 'wb') as fh:
                    fh.write(content)
                    result[sha1] = url
        return result

    def _crawl_single_url(self, url, out_dir):
        """
        url: where we start crawling, should be a complete URL like
        'http://www.intel.com/news/'
        """
        self.content = {}
        self._crawl([url], self.depth)
        result = self._save_crawl_content(out_dir)
        return result

    def crawl_multi_url(self, url_src, out_dir):
        urllist = []
        if isinstance(url_src, list):
            urllist = url_src
        elif os.path.isfile(url_src):
            with open(url_src, 'rb') as fh:
                for url in fh.readlines():
                    url = url.strip()
                urllist.append(url)

        total_result = {}
        for url in urllist:
            result = self._crawl_single_url(url, out_dir)
            total_result.update(result)
        return total_result



def save_crawl_result(result, file_path):
    with open(file_path, 'w') as fh:
        for sha1, url in result.items():
            fh.write("%s\t%s\n" %(sha1, url))

def crawl_url_by_single_thread(file_path, out_dir, crawl_depth, crawl_js):
    crawler = Crawler(crawl_depth, crawl_js)
    result = crawler.crawl_multi_url(file_path, out_dir)
    save_crawl_result(result, "crawl_sha1_url.txt")

def process_url_list_by_crawler(url_list, out_dir, crawl_depth, crawl_js, result_file_path):
    crawler = Crawler(crawl_depth, crawl_js)
    result = crawler.crawl_multi_url(url_list, out_dir)
    save_crawl_result(result, result_file_path)

def crawl_url_by_multi_thread(file_path, out_dir, crawl_depth, crawl_js):
    thread_num = multiprocessing.cpu_count() * thread_num_multiple
    
    # get url_list_map
    url_list_map = {}
    for i in range(0,thread_num):
        url_list_map[i] = []
    with open(file_path, 'r') as fh:
        i = 0
        for line in fh.readlines():
            url_list_map[i%thread_num].append(line.strip())
            i += 1
    
    # create url crawler multi process
    proc_list = []
    output_list = []
    for i in range(0,thread_num):
        result_file_path = os.path.join(os.path.dirname(out_dir), "crawl_sha1_url_" + str(i) + ".txt")
        output_list.append(result_file_path)
        proc = multiprocessing.Process(target=process_url_list_by_crawler, args=(url_list_map[i], out_dir, crawl_depth, crawl_js, result_file_path))
        proc_list.append(proc)

    for proc in proc_list:
        proc.start()
    # Wait for all threads to complete
    for proc in proc_list:
        proc.join()

    # merge all output
    with open('crawl_sha1_url.txt', 'wb') as dest_fh:
        for file_path in output_list:
            with open(file_path, 'rb') as fh:
                dest_fh.write(fh.read())
            os.remove(file_path)
    print "Exiting Main Process"
        


                
help_msg = """
crawler.py Usage:
    1. crawl url
       >> python crawler.py --file_path=FILE_PATH --dest_dir=DEST_DIR
    2. crawl url and inner js 
       >> python crawler.py --file_path=FILE_PATH --dest_dir=DEST_DIR --crawl_js=True
        
"""                

if __name__ == "__main__": 
    parser = OptionParser(usage=help_msg)
    parser.add_option("--crawl_js", dest="crawl_js", default='False',
                      help="specify crawl type", metavar="Trule|False")   
    parser.add_option("--file_path", dest="file_path",
                      help="specify crawl file path")
    parser.add_option("--dest_dir", dest="dest_dir",
                      help="specify destination path")
    #parser.add_option("--crawl_depth", dest="crawl_depth", default='1',
    #                  help="specify crawl depth")
    parser.add_option("--multi_thread", dest="multi_thread",
                      help="specify multi thread or not", metavar="Trule|False")

    (options, args) = parser.parse_args()
    # set config in logging
    basicConfig(filename='pysie.log', format='[%(asctime)s][%(levelname)s] - %(message)s', level=INFO)
    if options.crawl_js == 'True':
        crawl_js = True
        crawl_depth = 2
    elif options.crawl_js == 'False':
        crawl_js = False
        crawl_depth = 1

    if options.multi_thread:
        crawl_url_by_multi_thread(options.file_path, options.dest_dir, crawl_depth, crawl_js)
    else:
        crawl_url_by_single_thread(options.file_path, options.dest_dir, crawl_depth, crawl_js)
