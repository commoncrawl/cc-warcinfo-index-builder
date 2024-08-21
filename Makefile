.PHONEY: collinfo all-crawls all-workinfo parquet test

collinfo:
	wget https://index.commoncrawl.org/collinfo.json

all-crawls:
	cat collinfo.json | jq -r '.[].id' > all-crawls

all-warcinfo:
	cat all-crawls | xargs -n 1 -P 200 python make-warcinfo-index.py

parquet:
	python merge-parquets.py CC-MAIN-*.parquet

test:
	python -m pytest .
