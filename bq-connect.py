from google.cloud import bigquery
import os
import pandas
#from sqlalchemy import create_engine, text

client = bigquery.Client(project = 'test-project-78009')

#Getting Data from Bigquery into Python
sql = """SELECT *
		FROM sample_dataset.Data_Sample
		LIMIT 10;
		"""

query_job = client.query(sql)
results = query_job.result()

# for r in results:
# 	print(r.year, r.title, r.genre, r.avg_vote, r.movie_rating)

#Pushing Data from Python to Bigquery

target_table = 'test-project-78009.sample_dataset.push_data'

##setting up job config
job_config = bigquery.LoadJobConfig(
	skip_leading_rows = 1,
	source_format = bigquery.SourceFormat.CSV,
	autodetect = True
	)

#the file to export variables
cur_path = os.getcwd()
file_path2 = os.path.join(cur_path, 'data2.csv')

with open(file_path2, 'rb') as source_file:
	load_job = client.load_table_from_file(
		source_file,
		target_table,
		job_config= job_config)

load_job.result()

destination_table = client.get_table(target_table)

print(f" you have {destination_table.num_rows} rows")


