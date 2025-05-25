import sys
from io import BytesIO
import gzip
import os.path

import boto3
import botocore.exceptions
import pandas as pd
from warcio.recordloader import ArcWarcRecordLoader
from warcio.bufferedreaders import DecompressingBufferedReader
from tqdm import tqdm


#import warnings
#warnings.filterwarnings("ignore", message="numpy.dtype size changed")
#warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

s3_bucket = 'commoncrawl'
s3_prefix = 'crawl-data/{}/'

s3 = boto3.client('s3')


def get_paths_from_s3(s3_bucket, key):
    try:
        response = s3.get_object(
            Bucket=s3_bucket,
            Key=key,
        )
    except botocore.exceptions.ClientError as error:
        # this is expected for the 3 earliest crawls
        if repr(error).startswith('NoSuchKey'):
            print('No warc.paths.gz for', crawl)
            return
        raise
    except KeyboardInterrupt as error:
        print('^C fetching', s3_bucket, key, repr(error))
        return

    warcs = gzip.decompress(response['Body'].read()).decode('utf8').splitlines()
    return warcs


for crawl in sys.argv[1:]:
    crawl = crawl.rstrip()
    key = s3_prefix.format(crawl) + 'warc.paths.gz'
    localpath = crawl + '-warc.paths.gz'
    outname = crawl+'-warcinfo-id.parquet'
    if os.path.isfile(outname):
        # make this quiet because it's the normal case
        #print('skipping', outname, 'because it already exists.')
        continue
    print('processing', outname)

    # use paths from local disk if it exists
    if os.path.isfile(localpath):
        warcs = gzip.open(localpath, mode='rt').readlines()
        warcs = [w.rstrip() for w in warcs]
    else:
        warcs = get_paths_from_s3(s3_bucket, key)
    if not warcs:
        continue

    output = []

    for warc in tqdm(warcs):
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
