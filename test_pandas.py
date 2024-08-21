import pandas as pd


def load_df():
    print('read')
    df = pd.read_parquet('warcinfo-id.parquet')

    # if we read a df with a sorted index, lookups are slow
    # so apparently we need to do this here
    print('set index')
    df.set_index(df['warcinfo_id'], inplace=True)
    print('sort index')
    df.sort_index(inplace=True)
    print('done')
    return df


tests = [
    [
        'urn:uuid:fffff219-cd42-4897-bc76-e892d2f3d7c6',
        'crawl-data/CC-MAIN-2016-30/segments/1469257825124.22/warc/CC-MAIN-20160723071025-00142-ip-10-185-27-174.ec2.internal.warc.gz'
    ]
]


def test_lookup():
    df = load_df()

    for t in tests:
        print('lookup')

        try:
            match = df.at[t[0], 'warc_filename']
        except KeyError as e:
            print(repr(e))
            assert False, 'no match'

        assert match == t[1], 'incorrect result'
