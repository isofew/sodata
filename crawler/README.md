### Crawler script

This script is used to download reputation events data from stackoverflow website.

First, set up the sqlite3 database (to track visited urls):
```
sqlite3 urls.db <schema.txt
```

Then, populate the database with the user ids you want to download:
```
seq 1 15000000 | python3 insert.py
```

Finally, download their reputation tabs (don't forget to use some proxy pool):
```
env http_proxy= https_proxy= python3 download.py
```

Export data to json-per-line format:
```
python3 gen_reps_json.py urls.db 1 15000000 >/path/to/Reps.json
```
