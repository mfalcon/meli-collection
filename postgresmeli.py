# -*- coding: utf-8 -*-

import json

import os
import sys
import logging
import math
import multiprocessing
from datetime import datetime
from random import shuffle, randint
import psycopg2
from psycopg2.extensions import AsIs
from psycopg2.extras import Json


import meli_api

import beanstalkc

beanstalk = beanstalkc.Connection(host='localhost', port=11300)


INITIAL_OFFSET = 0
RES_LIMIT = 200
ITEMS_IDS_LIMIT = 50
BULK_ITEMS = 50
WORKERS_NUM = 10


ML_SITES = ['MLA','MLM','MLB']

ALLOWED_CATEGORIES = {'MLA': ['MLA1051','MLA1648','MLA1144','MLA1039',
    'MLA5726','MLA1000'], 'MLB': ['MLB1051', 'MLB1648','MLB5726','MLB1000'], 
    'MLM': ['MLM1051', 'MLM1648','MLM1574','MLM1144']}


def get_datetime(utc_diff=3): #argentina utc -3 
    return datetime.datetime.utcnow() - relativedelta(hours=utc_diff)


def get_logger(logtype, logname):
    LOG_DIR =  os.path.join(os.path.dirname(os.path.realpath(__file__)), 'logs/')
    LOG_PATH = os.path.join(LOG_DIR, logname)

    logger = logging.getLogger(logtype)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')

    fh = logging.FileHandler(LOG_PATH)
    fh.setLevel(logging.DEBUG)

    fh.setFormatter(formatter)

    logger.addHandler(fh)

    return logger


def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def create_job(target, *args):
    """
    creates a new job using the target function and passing the args
    """
    p = multiprocessing.Process(target=target, args=args)
    p.start()
    return p

   
class MeliCollector():
    
    def __init__(self, mapi, melidb='meli_testing'):
        self.conn = psycopg2.connect("dbname=%s" % melidb)
        self.cur = self.conn.cursor()
        
        self.mapi = mapi
        self.logger = get_logger('info', 'postgres')

    def get_conn(self, melidb='meli_testing'):
        self.conn = psycopg2.connect("dbname=%s" % melidb)
        self.cur = self.conn.cursor()
        self.bstk = beanstalk = beanstalkc.Connection(host='localhost', port=11300)


    def find_one(self, table, id_value):
        if table == 'seller':
            self.cur.execute("SELECT EXISTS(SELECT 1 FROM seller WHERE id=%d)" % id_value) 
        elif table == 'item':
            self.cur.execute("SELECT EXISTS(SELECT 1 FROM item WHERE id='%s')" % id_value) 
        elif table == 'category':
            self.cur.execute("SELECT EXISTS(SELECT 1 FROM category WHERE id=%s);", (id_value,))
        data = self.cur.fetchone()[0]
        if data:
            return data
        return None


    def get_one(self, table, id_value):
        if table == 'seller':
            self.cur.execute("SELECT * FROM seller WHERE id=%d" % id_value) 
        elif table == 'item':
            self.cur.execute("SELECT * FROM item WHERE id='%s'" % id_value) 

        data = self.cur.fetchone()[0]
        if data:
            return data
        return None
    
    
    def add_row(self, data, table): #data is a dictionary
        columns = data.keys()
        values = []
    
        for column in columns:
            if isinstance(data[column], list): #checking for json values
                values.append(Json(data[column])) 
            elif isinstance(data[column], dict):
                values.append(Json(data[column])) 
            else:
                values.append(data[column])
        insert_statement = 'insert into %s ' % table + '(%s) values %s'
        self.cur.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))
        self.conn.commit()
        print "added %s" % table
    
    
    def add_row_bulk(self, data, table): #data is a dictionary
        columns = data[0].keys()
        cols = (AsIs(','.join(columns)))
        value_rows = []
        for e in data:
            columns = e.keys()
            values = []
        
            for column in columns:
                if isinstance(e[column], list): #checking for json values
                    values.append(Json(e[column])) 
                elif isinstance(e[column], dict):
                    values.append(Json(e[column])) 
                else:
                    values.append(e[column])
            value_rows.append(tuple(values))        
      
        if table == 'item':
            first_arg = 'INSERT INTO item (%s) VALUES ' + ','.join(['%s'] * len(value_rows))
        elif table == 'item_status':
            first_arg = 'INSERT INTO item_status (%s) VALUES ' + ','.join(['%s'] * len(value_rows))
        second_arg = [cols] + value_rows
        query = self.cur.mogrify(first_arg, second_arg)
        self.cur.execute(query) 
        self.conn.commit()
        print "added %s" % table

    
    def get_leaf_cats(self, parent_categories):
        query = """select id from category
        where  to_json(array(select jsonb_array_elements(path_from_root) ->> 'id'))::jsonb ?|array%s
        and jsonb_array_length(children_categories) = 0;""" % parent_categories
        self.cur.execute(query) 
        leaf_cats = [cat[0] for cat in self.cur]
        return leaf_cats


    def get_pages(self, category_ids, limit=RES_LIMIT):
        '''receives a category_id and returns a list of page items
        to queue'''
        pages = []
        for cat_id in category_ids:
            
            items_data = self.mapi.search_by_category(cat_id[:3], cat_id, limit, 0)
            total_items = items_data['paging']['total']
            print "total items: %s" % total_items
            total_pages = total_items/items_data['paging']['limit'] + 1 #FIXME: RES_LIMIT not paging limit
            print "total pages: %s" % total_pages
            offset = 0
            for pn in range(total_pages):
                if pn == total_pages -1:
                    last = True #TODO: remove last field, useless here
                else:
                    last = False
                pages.append({'category_id': cat_id, 'limit': limit, 'offset': offset, 'last': last})
                offset += int(limit)
        
        return pages
        
    
    def insert_all_categories(self, cats):
        self.get_conn()
        for cat_id in cats:
            def _get_leaf_nodes(cat_id):
                cat_in_db = self.find_one('category', cat_id)
                cat_data = self.mapi.get_category(cat_id)
                if cat_in_db:
                    #check_if_changed(cat_in_db, cat_data)
                    pass
                else:
                    data = {k: cat_data[k] for k in ('id', 'name', 'path_from_root', 'children_categories')}
                    self.add_row(data, 'category')
                    print "added %s" % cat_data['name']
                    
                if len(cat_data['children_categories']) == 0:
                    #leaf cat, end of recursion
                    print "leaf cat"
                else:
                    for cat in cat_data['children_categories']:
                        _get_leaf_nodes(cat['id'])
            
            _get_leaf_nodes(cat_id)

 
    def insert_seller(self, seller_id):

        seller_in_db = self.find_one('seller', seller_id)
        if not seller_in_db:
            seller_data = self.mapi.get_seller_info(seller_id)
            
            data = {k: seller_data[k] for k in ('id',
            'nickname','registration_date','points','permalink',
            'user_type','address','seller_reputation',
            'tags','status')}
            
            self.add_row(data, 'seller')         
            

    def insert_item(self, item_source, pn):

        item_in_db = self.find_one('item', item_source['id'])
        to_add = False
        if not item_in_db:
            to_add = True
            item_source['product_id'] = None #TODO: check a cleaner solution
            
            item_data = {k: item_source[k] for k in ('id', 'seller_id', 
            'category_id', 'product_id', 'site_id', 'title', 'subtitle',
            'start_time','stop_time','permalink','condition',
            'initial_quantity','base_price','warranty','location',
            'shipping','pictures','geolocation',
            'listing_type_id','seller_address',
            'non_mercado_pago_payment_methods','parent_item_id')}
            
            print "adding item ************************************"
            
        #items status always gets inserted no matter if the item already exists
        item_status = self.insert_item_status(item_source, pn)
        if to_add:
            return item_data, item_status
        else:
            return None, item_status
    
    
    def insert_item_status(self, item, pn):
        st_item = {}
        st_item['item_id'] = item['id']
        st_item['created_time'] = datetime.now()
        st_item['available_quantity'] = int(item['available_quantity'])
        st_item['sold_quantity'] = int(item['sold_quantity'])
        st_item['price'] = float(item['price'])
        st_item['pn'] = pn
        
        return st_item
    
    def collect_items(self, pages):
        for page in pages:
            print page
            self.get_items(page)
           
    
    def items_collector(self):
        self.get_conn()
        self.logger.info("%s - working " % os.getpid())
        while True:
            job = self.bstk.reserve(timeout=30)
            if not job:
                break
            else:
                cat_page = json.loads(job.body)
                cat_id = cat_page['category_id']
                print cat_id
                job.delete()

                print "getting items"
                self.get_items(cat_page)
            

    def get_items(self, cat_data, limit=RES_LIMIT):
        """
        get all the items of a category for a given page with offset.
        The history of an item can be done matching records with the
        same id.
        """
        cat_id =  cat_data['category_id']
        limit =  cat_data['limit']
        offset = cat_data['offset']
        pn = offset/limit
        data = []
        items_data = self.mapi.search_by_category(cat_id[:3],cat_id, limit, offset)
        if len(items_data['results']) > 0:
            items = items_data['results'] 
            to_insert_items = []
            to_insert_status = []
            #separate into chunks and make a bulk call
            item_ids = [item['id'] for item in items]
            item_chunks = chunks(item_ids, ITEMS_IDS_LIMIT)
            for item_ids in item_chunks:
                items_data = self.mapi.get_items_data(item_ids)
                for item in items_data:
                    self.insert_seller(item['seller_id'])
                    item, item_status = self.insert_item(item, pn) 
                    if item:
                        to_insert_items.append(item)
                    to_insert_status.append(item_status)
        
            if len(to_insert_items) > 0:
                self.add_row_bulk(to_insert_items, 'item')
                self.logger.info("added in bulk: %d items" % len(to_insert_items))
            self.add_row_bulk(to_insert_status, 'item_status')
            self.logger.info("added in bulk: %d status" % len(to_insert_status))



def main(melidb='meli_testing', workers=4):
        if workers != 1:
            procs = []
            mapi = meli_api.MeliAPI()
            mc = MeliCollector(mapi, melidb)
            the_pool = multiprocessing.Pool(workers, mc.items_collector,)
            
            the_pool.close()
            the_pool.join()

        else:
            mapi = meli_api.MeliAPI()
            mc = MeliCollector(mapi, melidb)
            pages = mc.get_pages(['MLA1055'])
            mc.collect_items(pages)


if __name__ == '__main__':
    if sys.argv[1] == 'test':
        main(workers=1)
    else:
        main(workers=multiprocessing.cpu_count() * 2)
    
    sys.exit()

