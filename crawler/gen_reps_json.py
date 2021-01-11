import xml.etree.ElementTree as ET
import sodata.scripts
import sqlite3
import json
import sys
import re

from collections import defaultdict
from tqdm import tqdm

answer_re = re.compile('\#([0-9]+)$')
question_re = re.compile('^/questions/([0-9]+)')
user_re = re.compile('/users/([0-9]+)')

def add_post_id(d):
    if 'link' in d:
        a = answer_re.findall(d['link'])
        q = question_re.findall(d['link'])
        if len(a) > 0:
            d['PostTypeId'] = 2
            d['PostId'] = int(a[0])
        elif len(q) > 0:
            d['PostTypeId'] = 1
            d['PostId'] = int(q[0])
        del d['link']
    return defaultdict(lambda: None, d)

def trows(content):
    try:
        div = ET.fromstring(content)
    except Exception as e:
        return
    for table in div:
        for tbody in table:
            for tr in tbody:
                yield tr

def parse(user, pages, content, link=''):
    for tr in trows(content):
        d = dict()
        for td in tr:
            C = td.attrib['class']
            if C == 'rep-left':
                for span in td:
                    try:
                        d['Delta'] = int(span.text.replace('#x2B;', '+'))
                    except Exception as e:
                        d['Delta'] = str(e)
            elif C == 'rep-time':
                d['Time'] = td.attrib['title']
            elif C == 'rep-desc':
                d['Text'] = td.text
            elif C.startswith('rep-link'):
                for a in td:
                    d['link'] = a.attrib['href']
                if 'async-load' in C:
                    yield from parse(user, pages, pages[ td.attrib['data-load-url'] ], link=d['link'])
        if link != '':
            d['link'] = link # apply override
        if 'Text' in d and not d['Text'].endswith(' events'):
            d['UserId'] = user
            yield add_post_id(d)

def gen_user_reps(user_min, user_max, crawler_db):
    conn = sqlite3.connect(crawler_db)
    cur = conn.cursor()

    user_start = user_min
    user_end = min(user_max, 30000)

    while user_start <= user_end:
        rows = cur.execute(
            'select url,content from urls where url like "%/ajax/%" and user >= ? and user <= ?',
            (user_start, user_end)
        )

        pages = {}
        for url,content in tqdm(rows, desc=f'load user {user_start} to {user_end}'):
            s = content.replace('&', '')
            p = url.replace('https://stackoverflow.com', '')
            pages[p] = s

        for path in pages.keys():
            if '/expand/' not in path:
                user = int( user_re.findall(path)[0] )
                yield from parse(user, pages, pages[path])

        user_start = user_end + 1
        user_end = min(user_max, user_start * 3)

    conn.close()

if __name__ == '__main__':
    crawler_db = sys.argv[1]
    user_min = int(sys.argv[2])
    user_max = int(sys.argv[3])

    for d in tqdm(gen_user_reps(user_min, user_max, crawler_db), desc='output'):
        print(json.dumps(d))
