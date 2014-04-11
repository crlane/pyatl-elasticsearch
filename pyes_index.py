#!/usr/bin/env 
import gzip
import json
from pyes import ES, helpers
from contextlib import closing



def load_players(conn, doctype,index,filename, count=None, settings=None, mapping=None, drop=False):
    if drop:
        conn.indices.delete_index_if_exists(index)

    conn.indices.create_index(index, settings=settings)

    if filename.endswith('gz'):
        fp = gzip.GzipFile(filename)
    else: 
        fp = open(filename)
    with closing(fp) as fp:
        for i, line in enumerate(fp):
            if count and (i == count):
                break
            try:
                doc = json.loads(line)
                conn.index(doc, index, doctype, bulk=True)
            except:
                print 'Error loading line: {}'.format(i+1)
                import ipdb; ipdb.set_trace()
            if not i % 1000:
                print 'Loaded {} records'.format(i)
                conn.indices.refresh(index)

def main():
    SERVER_ADDRESS = ('http', '127.0.0.1', '9200')
    conn = ES(SERVER_ADDRESS)
    index_name = 'mlbplayers'
    document_type = 'player'
    filename = 'mlbplayerdata.json.gz'
    settings = {'index': {'refresh_interval': '-1'}}
    sb = helpers.SettingsBuilder(settings=settings)
    load_players(conn, document_type, index_name, filename, settings=sb, drop=True)

if __name__ == '__main__':

    main()