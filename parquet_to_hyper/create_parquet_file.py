import os
import pyarrow as pa
from pyarrow import csv as pacsv
from pyarrow import parquet as pq
from uuid import uuid4
import requests
import shutil

root_path = r'C:\p2h\sample'
# shutil.rmtree(root_path)
sample_path = os.path.join(root_path, str(uuid4()))

#create a random folder in c:\p2h\sample\{uuid4()}
os.makedirs(sample_path, exist_ok=True)

#download sample data from the internet 
data_url = 'https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2021-financial-year-provisional/Download-data/annual-enterprise-survey-2021-financial-year-provisional-csv.csv'
csv_path = os.path.join(r'C:\p2h', 'sample.csv')
if not os.path.isfile(csv_path):
    print('downloading the csv file')
    r = requests.get(data_url)
    with open(csv_path,'w') as f:
        f.write(r.text)
table = pacsv.read_csv(csv_path)
print('the csv file has', table.num_rows, 'rows')

#convert the data into parquet format
row_size = 10000
i = 0
for j in range(0, table.num_rows, row_size):
    chunk_parquet_path = os.path.join(sample_path, f'{i:08}.parquet')
    chunk = table.slice(j, min(row_size, table.num_rows - j))
    pq.write_table(chunk, chunk_parquet_path)
    i += 1

output_table = pq.read_table(sample_path)
print('the parquet files have', output_table.num_rows, 'rows')
print('sample parquet files have been created at:', sample_path)