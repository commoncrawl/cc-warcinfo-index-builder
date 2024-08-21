import duckdb


tests = [
    [
        'urn:uuid:fffff219-cd42-4897-bc76-e892d2f3d7c6',
        'crawl-data/CC-MAIN-2016-30/segments/1469257825124.22/warc/CC-MAIN-20160723071025-00142-ip-10-185-27-174.ec2.internal.warc.gz'
    ]
]


sql = '''
select
  warc_filename
from
  read_parquet('warcinfo-id.parquet')
where
  warcinfo_id = '{}'
'''


def test_db():
    for t in tests:
        res = duckdb.sql(sql.format(t[0])).fetchall()  # 0.4 seconds
        #res = duckdb.execute(sql, [t[0]]).fetchall()  # 1.6 seconds ?!
        print(res)
        assert res[0][0] == t[1]
