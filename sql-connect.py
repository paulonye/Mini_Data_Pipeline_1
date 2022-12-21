# IMPORT THE SQALCHEMY LIBRARY's CREATE_ENGINE METHOD
from sqlalchemy import create_engine, text
import mysql.connector
import pandas as pd
import os
from mysql.connector import errorcode
  
#DEFINE THE DATABASE CREDENTIALS
user = 'oscarval_user'
password = 'learnsql123'
host = 'oscarvalles.com'
port = 3306
database = 'oscarval_ddl_sandbox'


cur_path = os.getcwd()
file_path1 = os.path.join(cur_path, 'data1.csv')
file_path2 = os.path.join(cur_path, 'data2.csv')
  
# PYTHON FUNCTION TO CONNECT TO THE MYSQL DATABASE AND
# RETURN THE SQLACHEMY ENGINE OBJECT

def get_connection():
    #The url string used below is unique to mysql
    #I had to install the pymysql driver which is
    #the DB API that moves information between SQLAlchemy and the mysql database.
    return create_engine(
        url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )
  

engine = get_connection()
print(
f"Connection to the {host} for user {user} created successfully.")

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

query_2 = """ SELECT *
        FROM oscarval_sql_course.city_house_prices;
        """




df1 = pd.read_sql(query_1, engine)

df2 = pd.read_sql(query_2, engine)


df2.set_index('Date', inplace = True)
df2 = df2.stack().reset_index()

df2.columns = ['date', 'city', 'price']

print(df2.columns)

df1.to_csv(file_path1)
#df2.to_csv(file_path2)
print('close connection')
engine.connect().close()