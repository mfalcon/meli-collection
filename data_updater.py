# -*- coding: utf-8 -*-

import json

import os
import sys
from datetime import datetime

import psycopg2
from psycopg2.extensions import AsIs
from psycopg2.extras import Json


import meli_api


ML_SITES = ['MLM','MLB','MLA']

ALLOWED_CATEGORIES = {'MLA': ['MLA1051','MLA1648','MLA1144','MLA1039',
    'MLA5726','MLA1000'], 'MLB': ['MLB1051', 'MLB1648','MLB5726','MLB1000'], 
    'MLM': ['MLM1051', 'MLM1648','MLM1574','MLM1144']}
    

class MeliUpdater():
    def __init__(self, mapi, melidb='meli_testing'):
        self.conn = psycopg2.connect("dbname=%s" % melidb)
        self.cur = self.conn.cursor()
        
        self.mapi = mapi


    def find_one(self, table, id_value):
        if table == 'category':
            self.cur.execute("SELECT EXISTS(SELECT 1 FROM category WHERE id=%s);", (id_value,))
        elif table == 'item_descriptions':
            self.cur.execute("SELECT EXISTS(SELECT 1 FROM item_descriptions WHERE item_id=%s);", (id_value,))
        data = self.cur.fetchone()
        if data:
            return data[0]
        return None

    
    def add_row(self, data, table): #data is a dictionary
        columns = data.keys()
        values = []
        for column in columns:
            if type(data[column]) is list:
                values.append(Json(data[column]))
            else:
                values.append(data[column])
                
        insert_statement = 'insert into %s ' % table + '(%s) values %s'
        self.cur.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))
        self.conn.commit()

   
    def check_if_changed(self, cat_in_db, cat_data):
        '''checks if the category has changed its path_from_root
        and/or its children_categories. If changed return the new values
        to update, if not return False'''
        pass
        
        
    def insert_all_categories(self, cats):
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
                
    
    
    def get_leaf_cats(self, parent_categories): #parent_categories is a list of category ids
        query = """select id from category
        where  to_json(array(select jsonb_array_elements(path_from_root) ->> 'id'))::jsonb ?|array%s
        and jsonb_array_length(children_categories) = 0;""" % parent_categories
        self.cur.execute(query) #TODO: learn well this
        leaf_cats = [cat[0] for cat in self.cur]
        return leaf_cats
    
        
    
    def check_seller_updates(self):
        pass
                
    
    
def main():       
    for site_id in ML_SITES:
        mapi = meli_api.MeliAPI()
        mu = MeliUpdater(mapi)
        mu.insert_all_categories(ALLOWED_CATEGORIES[site_id])        
    
    
if __name__ == '__main__':
    main()
