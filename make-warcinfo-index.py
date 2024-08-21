import sys
from io import BytesIO
import gzip
import os.path

import boto3
import botocore.exceptions
import pandas as pd
from warcio.recordloader import ArcWarcRecordLoader
from warcio.bufferedreaders import DecompressingBufferedReader


#import warnings
#warnings.filterwarnings("ignore", message="numpy.dtype size changed")
#warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

s3_bucket = 'commoncrawl'
s3_prefix = 'crawl-data/{}/'

s3 = boto3.client('s3')

for crawl in sys.argv[1:]:
    crawl = crawl.rstrip()
    key = s3_prefix.format(crawl) + 'warc.paths.gz'
    outname = crawl+'-warcinfo-id.parquet'
    if os.path.isfile(outname):
        # make this quiet because it's the normal case
        #print('skipping', outname, 'because it already exists.')
        continue
    print('processing', outname)

    try:
        response = s3.get_object(
            Bucket=s3_bucket,
            Key=key,
        )
    except botocore.exceptions.ClientError as error:
        # this is expected for the 3 earliest crawls
        if repr(error).startswith('NoSuchKey'):
            print('No warc.paths.gz for', crawl)
            continue
        raise
    except KeyboardInterrupt as error:
        print('^C fetching', s3_bucket, key, repr(error))
        continue

    warcs = gzip.decompress(response['Body'].read()).decode('utf8').splitlines()

    output = []

    for warc in warcs:
        try:
            response = s3.get_object(
                Bucket=s3_bucket,
                Key=warc,
                Range='bytes=0-16383',
            )
        except botocore.exceptions.ClientError as error:
            print('No such key', s3_bucket, warc, repr(error))
            continue
        except KeyboardInterrupt as error:
            print('^C fetching', s3_bucket, warc, repr(error))
            continue

        record_bytes = response['Body'].read()  # warcio will decompress for us
        stream = DecompressingBufferedReader(BytesIO(record_bytes))
        try:
            record = ArcWarcRecordLoader().parse_record_stream(stream)
        except EOFError:
            print('too-long warcinfo record in '+warc)
            raise

        assert record.rec_headers['WARC-Type'] == 'warcinfo'
        warcinfo = record.rec_headers['warc-record-id'].strip('<>')

        record = {
            'warcinfo_id': warcinfo,
            'warc_filename': warc,  # WARC-Filename is just the last part
        }

        output.append(record)

    df = pd.DataFrame(output)

    df = df.sort_values('warcinfo_id')
    df.to_parquet(outname, index=False)
