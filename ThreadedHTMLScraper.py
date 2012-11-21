#!/usr/bin/env Python
import sys, threading, Queue, time, urllib
from HTMLParser import HTMLParser

class ThreadedHTMLScraper(object):
    def __init__(self, urls, limit = 100): 
        '''
            Creates a new ThreadedHTMLScraper which will return the HTML
            from a list of urls passed in.
            Params: urls - a list of properly formatted urls
                    limit - the amount of concurrent URL requests allowed
                            default is set to 10
        '''
        self.url_list = urls        # properly formatted urls only
        self.max_concurrent_threads = limit
        self.lock = threading.Lock()  # lock for granding access to write list
        self.html_dict = {}         # a dictionary to hold results
        self.jobs = len(urls)

    def start(self):
        ''' returns a dictionary with html content from the urls
        '''
        print 'Starting ThreadedHTMLScraper :: Limit set to %d' % self.max_concurrent_threads
        start = time.time() * 1000
        
        def producer(q, urls):
            for url in self.url_list:
                thread = Worker(url)
                thread.start()
                q.put(thread, True)

        def consumer(q, job_count):
            while len(self.html_dict) < job_count:
                thread = q.get(True)
                thread.join()
                result = thread.get_html()
                self.html_dict[result[0]] = result[1]

        # Start the work
        q = Queue.Queue()
        prod_threads = threading.Thread(target=producer, args=(q, self.url_list))
        cons_threads = threading.Thread(target=consumer, args=(q, len(self.url_list)))

        prod_threads.start()
        cons_threads.start()
        prod_threads.join()
        cons_threads.join()

        elapsed = time.time() * 1000 - start
        print 'Completed query in %d milliseconds' % elapsed
        return self.html_dict
        
class Worker(threading.Thread):
    def __init__(self, url):
        self.url = url
        self.html = None
        threading.Thread.__init__(self)

    def get_html(self):
        return self.url, self.html

    def run(self):
        try:
            print 'Reading from %s' % self.url
            f = urllib.urlopen(self.url)
            content = f.read()
            f.close()
            self.html = content
        except IOError:
            print 'Could not open url: %s' % self.url
        
