#pages_collector
import json
import multiprocessing


import beanstalkc
import psycopg2

import meli_api


beanstalk = beanstalkc.Connection(host='localhost', port=11300)
conn = psycopg2.connect("dbname=%s" % 'meli_testing')
cur = conn.cursor()

ML_SITES = ['MLA'] #,'MLB','MLM']

ALLOWED_CATEGORIES = {'MLA': ['MLA1051','MLA1648','MLA1144','MLA1039',
    'MLA5726','MLA1000'], 'MLB': ['MLB1051', 'MLB1648','MLB5726','MLB1000'], 
    'MLM': ['MLM1051', 'MLM1648','MLM1574','MLM1144']}
    

def create_job(target, *args):
    """
    creates a new job using the target function and passing the args
    """
    p = multiprocessing.Process(target=target, args=args)
    p.start()
    return p


def get_leaf_cats(parent_categories):
    query = """select id from category
    where  to_json(array(select jsonb_array_elements(path_from_root) ->> 'id'))::jsonb ?|array%s
    and jsonb_array_length(children_categories) = 0;""" % parent_categories
    cur.execute(query) #TODO: learn well this
    leaf_cats = [cat[0] for cat in cur]
    return leaf_cats
    

def get_pages(category_ids, mapi, limit=200):
    '''receives a category_id and returns a list of page items
    to queue'''

    for cat_id in category_ids:
        
        items_data = mapi.search_by_category(cat_id[:3], cat_id, limit, 0)
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
            page_item = {'category_id': cat_id, 'limit': limit, 'offset': offset, 'last': last}
            beanstalk.put(json.dumps(page_item))
            print "page item: %s" % json.dumps(page_item) 
            offset += int(limit)


def items_collector():
    print os.getpid(),"working"
    self.get_conn()
    while True:
        job = self.bstk.reserve(timeout=30)
        if not job:
            break
        else:
            cat_page = json.loads(job.body)
            cat_id = cat_page['category_id']
            job.delete()

            print "getting items"
            self.get_items(cat_page)

    
def main():
    for site_id in ML_SITES:
        mapi = meli_api.MeliAPI()
        all_cats = get_leaf_cats(ALLOWED_CATEGORIES[site_id])
        all_pages = get_pages(all_cats, mapi) #conseguir todas las paginas de las categorias

        

if __name__ == '__main__':
    main()
        
