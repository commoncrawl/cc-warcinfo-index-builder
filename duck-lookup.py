import glob

import duckdb


warcinfo_index = duckdb.read_parquet('warcinfo-id.parquet')  # hive_partitioning=True

print('total records:')
print(duckdb.sql('SELECT COUNT(*) FROM warcinfo_index;'))

sql = '''
select
  warc_filename
from warcinfo_index
where
  warcinfo_id = 'urn:uuid:fffff219-cd42-4897-bc76-e892d2f3d7c6'
'''

print(duckdb.sql(sql))

#assert match == 'crawl-data/CC-MAIN-2016-30/segments/1469257825124.22/warc/CC-MAIN-20160723071025-00142-ip-10-185-27-174.ec2.internal.warc.gz'

