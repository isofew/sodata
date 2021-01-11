import logging
logger = logging.getLogger('sodata-download')
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

from tqdm import tqdm
from multiprocessing.pool import ThreadPool
import requests
import sqlite3
import re
import time

expand_event_re = re.compile(
    '/ajax/users/[0-9]+/rep/expand/[0-9]+/[0-9]+\?postId=[0-9]+'
)
expand_day_re = re.compile(
    '/ajax/users/[0-9]+/rep/day/[0-9]+\?sort=post' 
)
page_re = re.compile(
    '/users/[0-9]+/[^\?]*\?tab=reputation&sort=post&page=[0-9]+'
)

def parse_path(s):
    yield from expand_event_re.findall(s)
    yield from expand_day_re.findall(s)
    yield from filter(lambda x: not x.endswith('page=1'), page_re.findall(s))

def parse(s):
    for p in parse_path(s):
        yield 'https://stackoverflow.com' + p

TIMEOUT = 5
UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'

# task per row (url)
def task(row):
    url, user, *_ = row
    try:
        resp = requests.get(url, timeout=TIMEOUT, headers={'User-Agent': UA})
    except (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.ProxyError,
        requests.exceptions.SSLError,
        requests.exceptions.ChunkedEncodingError,
    ) as e:
        logger.debug(str(e))
        return []

    cmds = []

    if resp.ok or resp.status_code == 404:
        cmds.append((
            'update urls set downloaded = true, content = ? where url = ?',
            (resp.text if '/ajax/' in url else '', url)
        ))
        for new_url in parse(resp.text):
            cmds.append((
                'insert or ignore into urls (url, user, downloaded) values (?, ?, false)',
                (new_url, user)
            ))
    else:
        logger.warning(f'status code {resp.status_code} {url}')

    return cmds

def spaced_gen(xs, intvl):
    for x in xs:
        yield x
        time.sleep(intvl)

def download(urls_db_file, n_threads, row_limit, intvl):
    conn = sqlite3.connect(urls_db_file)
    cur = conn.cursor()

    while True:
        # display stats
        n_not_downloaded, *_ = cur.execute(
            'select count(1) from urls where downloaded = false'
        ).fetchone()
        n_downloaded, *_ = cur.execute(
            'select count(1) from urls where downloaded = true'
        ).fetchone()
        logger.info(f'not/downloaded: {n_not_downloaded}/{n_downloaded}')

        # fetch not downloaded rows & order by user
        rows = cur.execute(
            'select url, user from urls where downloaded = false order by user limit ?',
            (row_limit,)
        ).fetchall()

        if len(rows) == 0:
            logger.info('finished')
            return
        else:
            logger.info(f'now on user {rows[0][1]} ~ {rows[-1][1]}')
            # download in parallel threads, update in main thread
            with ThreadPool(n_threads) as pool:
                rows_gen = spaced_gen(rows, intvl)
                cmdss = list(tqdm( pool.imap_unordered(task, rows_gen) ))
                for cmds in cmdss:
                    for cmd in cmds:
                        cur.execute(*cmd)
            conn.commit()

if __name__ == '__main__':
    download(urls_db_file='urls.db', n_threads=300, row_limit=10000, intvl=1e-3)
