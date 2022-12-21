from sqlalchemy import create_engine, text
import pandas as pd
from google.cloud import bigquery
 
# DEFINE THE DATABASE CREDENTIALS
cred = []
with open('credentials.txt', 'r') as file:
	for line in file:
		cred.append(line.strip())

user = cred[0]
password = cred[1]
host = cred[2]
port = 3306
database = cred[4]


#construct a bigquery client object
client = bigquery.Client(project = 'test-project-78009')

def get_connection():
    return create_engine(
        url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )

engine = get_connection()

query_1 = """SELECT year, title, genre, avg_vote,
        CASE
        WHEN avg_vote < 3 THEN 'bad'
        WHEN avg_vote < 6 THEN 'okay'
        WHEN avg_vote >= 6 THEN 'good'
        END AS movie_rating,
        duration
        FROM oscarval_sql_course.imdb_movies
        WHERE year BETWEEN 2005 AND 2010;
        """
data = pd.read_sql(query_1, engine)

print(data.head(3))

#Loading into Big Query
target_table = 'test-project-78009.sample_dataset.push_data'
#tableid = project.dataset.table_name

##setting up job config
job_config = bigquery.LoadJobConfig(
	autodetect = True,
	write_disposition = 'WRITE_TRUNCATE'
	)

#Loading the Data
print('About to Load Data')
#loading data from a dataframe
#load_jon takes in three parameters: 
#- source, target, job_config
load_job = client.load_table_from_dataframe(
	data,
	target_table,
	job_config= job_config
	)

load_job.result()

#testing 
destination_table = client.get_table(target_table)

print(f" you have {destination_table.num_rows} rows")


