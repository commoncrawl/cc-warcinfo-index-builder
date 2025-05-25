.PHONY: collinfo all-crawls make-paths one-warcinfo all-warcinfo parquet test

collinfo:
	wget https://index.commoncrawl.org/collinfo.json

all-crawls:
	cat collinfo.json | jq -r '.[].id' > all-crawls

CRAWL=CC-MAIN-2025-21
make-paths:
	aws s3 ls --recursive s3://commoncrawl/crawl-data/$(CRAWL) | grep warc | awk '{print $$4}' | gzip > $(CRAWL)-warc.paths.gz

one-warcinfo:
	python make-warcinfo-index.py $(CRAWL)

all-warcinfo:
	cat all-crawls | xargs -n 1 -P 200 python make-warcinfo-index.py

parquet:
	python merge-parquets.py CC-MAIN-*.parquet

test:
	python -m pytest .

install:
	cp warcinfo-id.parquet /home/cc-pds/warcinfo-id.parquet
