#a kind of python meli api
import json
import logging
import time

import requests
from requests_ssl_fix import SSLAdapter
from requests.adapters import HTTPAdapter

import ssl
requests.packages.urllib3.disable_warnings()

req_s = requests.Session()
req_s.mount('https://', SSLAdapter(ssl.PROTOCOL_TLSv1))

#req_s = requests.Session()
#req_s.mount('https://', HTTPAdapter(max_retries=8))

BASE_URL = 'https://api.mercadolibre.com/'
BASE_SITE_URL = BASE_URL + 'sites/'
SLEEP_TIME = 0.1 #in seconds
ERROR_SLEEP_TIME = 0.1



class MeliAPI():
    def __init__(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        # create a file handler
        handler = logging.FileHandler('logs/mapi.log')
        handler.setLevel(logging.INFO)
        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(handler)
        self.logger = logger
        
    
    #TODO: replace it with requests retry
    def make_call(self, url, params=None):
        time.sleep(SLEEP_TIME)
        for i in range(10):
            if i != 0:
                self.logger.info("%s - retrying... %d" % (url,i))
                time.sleep(ERROR_SLEEP_TIME*i)
            try:
                res = requests.get(url, verify=False)
            except requests.ConnectionError, e:
                self.logger.info(e)
                continue
            
            if res.status_code == 200:
                #data = json.loads(res.text)
                data = res.json()
                if data:
                    return data
                continue

                
    #TODO: replace it with requests retry
    def make_call_v2(self, url, params=None):
        time.sleep(SLEEP_TIME)
        for i in range(10):
            if i != 0:
                self.logger.info("%s - retrying... %d" % (url,i))
                time.sleep(ERROR_SLEEP_TIME*i)
            try:
                res = requests.get(url)
            except requests.ConnectionError, e:
                self.logger.info(e)
                continue
            
            if res.status_code == 200:
                #data = json.loads(res.text)
                data = res.json()
                return data


    def get_seller_info(self, seller_id):
        url = BASE_URL + 'users/%s' % seller_id
        self.logger.info(url)
        data = self.make_call(url)
        return data


    def get_item_description(self, item_id):
        url = BASE_URL + 'items/%s/descriptions' % (item_id)
        print url
        self.logger.info(url)
        data = self.make_call_v2(url)
        return data


    def get_item_visits(self, item_id, date_from, date_to):
        #https://api.mercadolibre.com/items/{Items_id}/visits?date_from=2014-06-01T00:00:00.000-00:00&date_to=2014-06-10T00:00:00.000-00:00'
        url = BASE_URL + 'items/%s/visits?&date_from=%s&date_to=%s' % (item_id, date_from, date_to)
        print url
        self.logger.info(url)
        data = self.make_call(url)
        return data

    
    def get_items_visits(self, ids_list, date_from, date_to): #bulk results
        #https://api.mercadolibre.com/items/{Items_id}/visits?date_from=2014-06-01T00:00:00.000-00:00&date_to=2014-06-10T00:00:00.000-00:00'
        url = BASE_URL + 'items/visits?&date_from=%s&date_to=%s&ids=%s' % (date_from, date_to, ",".join(ids_list))
        self.logger.info(url)
        data = self.make_call(url)
        return data

        
    def get_items_data(self, items_ids):
        #Retrieves the information of a list of items: GET/items?ids=:ids
        url = BASE_URL + 'items/?ids=%s' % ",".join(items_ids)
        self.logger.info(url[:50])
        while True:
            data = self.make_call(url)
            try:
                data[0]
            except:
                print "******************ERROR*********************"
                print url
                continue
                
            return data


    def search_by_category(self, site_id, cat_id, limit, offset):
        #get the category items
        url = BASE_SITE_URL + '%s/search?category=%s&limit=%s&offset=%s&condition=new' % (site_id, cat_id, limit, offset)
        self.logger.info(url)
        while True:
            data = self.make_call(url)
            try:
                data['results']
            except:
                print "****************ERROR********************"
                print url
                continue
                
            return data


    def search_item(self, site_id, query):
        url = BASE_SITE_URL + '%s/search?q=%s' % (site_id, query)
        data = self.make_call(url)
        return data


    def get_category(self, cat_id):
        """
        get category info
        """
        url = BASE_URL + 'categories/%s' % cat_id
        data = self.make_call(url)
        return data
