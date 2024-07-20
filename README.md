# generate-warcinfo-index

For each crawl, generate parquet which has the following fields:

- warcinfo_id
- filename

Hive partition by crawl. Explore if lookups by warcinfo\_id are sped up by
have partitioning on a prefix, like Jason did for warc\_record\_id.

"fastwarc extract s3://warc-prefix/foo.warc.gz 0" gives you the warcinfo\_id



there's one warc that I can't read the head of

21:47 start for the latest index build
earliest finish 23:23, last finish 04:43
