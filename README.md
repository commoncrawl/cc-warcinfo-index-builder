# generate-warcinfo-index

For each crawl, generate parquet which has the following fields:

- warcinfo_id
- warc_filename

The `make all-warcinfo` step runs one extractor per crawl. On the
first run, the first crawl extraction finished in 1h 35m and the last
in 6h 56m.

A copy of the actual index can be found on rf:/home/cc-pds/warcinfo-id.parquet

## How to query

Look at the test code, test_pandas.py and test_duck.py

## Updating the index

The code uses smart_open() to read the initial part of every warc, extracting
the first record, which should be the warcinfo record.

The code is smart enough to not re-download anything, and runs in
parallel for every crawl. It only needs about 3% of a core per
extractor, but network latency slows it down to as slow as 7 hours for
a single crawl. And if you are doing many crawls in parallel, the
slowest one could be much slower than the fastest.

```
make collinfo
make all-crawls
make all-warcinfo
make parquet
make test
```

To add a single new crawl, edit the Makefile to change the CRAWL
variable, then

```
make one-paths
make one-warcinfo
make parquet
make test
```

## Install

If happy, copy to place:

```
make install
```
