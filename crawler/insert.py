import sys
from tqdm import tqdm

import sqlite3
conn = sqlite3.connect('urls.db')
cur = conn.cursor()
commit_size = 1000
buffer_size = 0

for line in tqdm(sys.stdin):
    uid = line.strip()
    cur.execute(
        'insert or ignore into urls (url, user, downloaded) values (?, ?, false)',
        (f'https://stackoverflow.com/users/{uid}/?tab=reputation', uid)
    )
    buffer_size += 1
    if buffer_size >= commit_size:
        conn.commit()
        buffer_size = 0

if buffer_size > 0:
    conn.commit()
    buffer_size = 0

conn.close()
