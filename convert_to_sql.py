from io import StringIO
import sys
import os

import boto3
import pandas as pd
import sqlalchemy

from set_env import set_env_from_env_file

set_env_from_env_file()

if len(sys.argv) < 3:
	print('wrong arguments')
	exit(1)

s3 = boto3.resource('s3', 
	aws_access_key_id=os.getenv('ACCESS_KEY'),
	aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY')
)

db_provider = os.getenv('DB_PROVIDER', 'postgresql')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT', 5432)
db_maintenance_db = os.getenv('DB_MAINTENANCE_DB', 'postgres')

bucket_name = sys.argv[1]
file_name = sys.argv[2]

engine = sqlalchemy.create_engine(f'{db_provider}://'
	f'{db_user}:'
	f'{db_password}@'
	f'{db_host}:'
	f'{db_port}/'
	f'{db_maintenance_db}'
)

obj = s3.Object(bucket_name, f'Production/{file_name}')
df = pd.read_csv(StringIO(obj.get()['Body'].read().decode('utf-8')),
             low_memory=False, index_col=0)

s3.Object(bucket_name, f'Processed/{file_name}')\
	.copy_from(CopySource={'Bucket': bucket_name, 'Key': f'Production/{file_name}'})

obj.delete()

df.to_sql(
	f'{file_name}_raw',
	engine,
	if_exists='replace',
	index_label='index',
	chunksize=1000
)
