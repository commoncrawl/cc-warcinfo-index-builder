# generate-warcinfo-index

For each crawl, generate parquet which has the following fields:

- warcinfo_id
- warc_filename

The `make all-warcinfo` step runs one extractor per crawl, and
the first finished in 1h 35m and the last in 6h 56m.
