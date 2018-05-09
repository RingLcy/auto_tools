# -*- coding: utf-8 -*-
# filename: crawler.py

import sqlite3  
import urllib2  
import os, sys
import multiprocessing
from HTMLParser import HTMLParser  
from urlparse import urlparse
from hash_helper import HashHelper

class MyHTMLParser(HTMLParser):  
    """
    Parser that extracts hrefs
    """
    def __init__(self):
        HTMLParser.__init__(self)
        self.js_links = set()
    
    def handle_starttag(self, tag, attrs):
        if tag in ('iframe', 'script'):
            dict_attrs = dict(attrs)
            if dict_attrs.get('src'):
                self.js_links.add(dict_attrs['src'])
    
class Crawler(object):  
    def __init__(self, depth=1):
        """
        depth: how many time it will bounce from page one (optional)
        cache: a basic cache controller (optional)
        """
        self.depth = depth
        self.content = {}

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

    def _crawl(self, urls, max_depth):
        n_urls = set()
        if max_depth:
            for url in urls:
                # do not crawl twice the same page
                if url not in self.content:
                    html = self._get(url)
                    self._set(url, html)
                    n_urls = n_urls.union(self.get_js_links(html, url))
                    #print n_urls
            self._crawl(n_urls, max_depth-1)

    def get_js_links(self, html, referer_url):
        result = set()
        u_parse = urlparse(referer_url)
        domain = u_parse.netloc
        scheme = u_parse.scheme

        html_parser = MyHTMLParser()
        html_parser.feed(html)
        for js_link in html_parser.js_links:
            if '.js' not in js_link:
                continue
            u_parse = urlparse(js_link)
            if not u_parse.scheme:
                if js_link.startswith('/'):
                    result.add(scheme + "://" + domain + u_parse.path)
                else:
                    result.add(scheme + "://" + domain + '/' + u_parse.path)
            else:
                result.add(js_link)
        return result

    def save_crawl_result(self, out_dir):
        hash_helper = HashHelper()
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        with open(os.path.join(out_dir, 'crawl_result.txt'), 'a+') as rh:
            for url in self.content:
                content = self.content[url]
                sha1 = hash_helper.calc_sha1(content)
                file_path = os.path.join(out_dir, sha1+'.html')
                if not os.path.exists(file_path) and len(content):
                    with open(file_path, 'wb') as fh:
                        fh.write(content)
                    rh.write("%s\t%s\n" %(sha1, url))

    def crawl_single_url(self, url, out_dir):
        """
        url: where we start crawling, should be a complete URL like
        'http://www.intel.com/news/'
        """
        self.content = {}
        self._crawl([url], self.depth)
        self.save_crawl_result(out_dir)

    def crawl_multi_url(self, url_list, out_dir):
        urllist = []
        if isinstance(url_list, list):
            process_urllist = url_list
        elif os.path.isfile(url_list):
            with open(url_list, 'rb') as fh:
                for url in fh.readlines():
                    url = url.strip()
                process_urllist.append(url)
        for url in process_urllist:
            self.crawl_single_url(url, out_dir)
    

def process_url_list_by_crawler(url_list, out_dir, depth):
    crawler = Crawler(depth)
    crawler.crawl_multi_url(url_list, out_dir)

def crawl_url_by_multi_thread(url_path, out_dir, depth=2):
    thread_num = multiprocessing.cpu_count() * 5
    #thread_num = 1
    url_list_map = {}
    for i in range(0,thread_num):
        url_list_map[i] = []
    with open(url_path, 'r') as fh:
        i = 0
        for line in fh.readlines():
            url_list_map[i%thread_num].append(line.strip())
            i += 1
    
    #
    proc_list = []
    for i in range(0,thread_num):
        proc = multiprocessing.Process(target=process_url_list_by_crawler, args=(url_list_map[i], os.path.join(out_dir, "process_thread_" + str(i)), depth))
        proc_list.append(proc)

    for proc in proc_list:
        proc.start()

    # Wait for all threads to complete
    for proc in proc_list:
        proc.join()
        


                
help_msg = """
crawler.py Usage:
    >> python crawler.py 
        
      

"""                

if __name__ == "__main__": 
    #crawler = Crawler(depth=2)
    #crawler.crawl_multi_url(['http://www.baidu.com/'], 'test', depth=2)
    crawl_url_by_multi_thread('top-1m.csv', sys.argv[1], depth=2)