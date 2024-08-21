import pandas as pd


def test_lookup():
    print('read')
    df = pd.read_parquet('warcinfo-id.parquet')

    # if we read a df with a sorted index, lookups are slow
    # so apparently we need to do this here
    print('set index')
    df.set_index(df['warcinfo_id'], inplace=True)
    print('sort index')
    df.sort_index(inplace=True)

    print('lookup')
    wi = 'urn:uuid:fffff219-cd42-4897-bc76-e892d2f3d7c6'
    try:
        match = df.at[wi, 'warc_filename']
    except KeyError as e:
        print('no match')
        print(repr(e))
        exit(1)

    print('match', match)
    assert match == 'crawl-data/CC-MAIN-2016-30/segments/1469257825124.22/warc/CC-MAIN-20160723071025-00142-ip-10-185-27-174.ec2.internal.warc.gz'

