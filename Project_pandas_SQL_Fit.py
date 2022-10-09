#!/usr/bin/env python
# coding: utf-8

#     # Task 1 - Fitbit Dataset

# In[76]:


#1. Read this dataset in pandas , mysql and mongodb 
#Reading file in pandas 
import pandas as pd 
df= pd.read_csv(r' data.csv', encoding='utf-8')
df.head()


# In[77]:


#Reading file in sql before reading data load the file in task3 
import mysql.connector as con
import pandas as pd
try:
    mydb = con.connect(host='localhost', user='root', passwd ='')
    cursor = mydb.cursor()
    creat= """create database if not exists fitbit"""
    cursor.execute(creat)
    cursor.execute("use fitbit")
    df1 = pd.read_sql("select *from fitbitdata ", mydb)
    df1.head(2)
except Exception as e:
    print(e)


# In[78]:


df1.head(2)


# In[79]:


#laod data in pmongodb
import pymongo
import json
client = pymongo.MongoClient("mongodb+srv://et/?r")
db = client.test
db1 = client['Fitbit']
coll = db1['Fitbdata']
df_js = df1.to_json('fitbitdata.json') #create jason file
with open('fitbitdata.json') as file:
    file_data = json.load(file)

# Inserting the loaded data in the Collection
# if JSON contains data more than one entry
# insert_many is used else insert_one is used

if isinstance(file_data, list):
    coll.insert_many(file_data)
else:
    coll.insert_one(file_data)


# In[80]:


#Read data in from mongo DB,loaded csv into mongodb
import pymongo
client = pymongo.MongoClient("ity")
db = client.test
db1 = client['Fitbit']
coll = db1['Fitbdata']

data_MD = coll.find()
for data in data_MD:
    print(data)


# In[75]:


# 2. while creting a table in mysql dont use manual approach to create it  ,always use a automation to create a table in mysql 


# In[59]:


import os , pymysql
import sqlalchemy as sq

#DB connection
try:
    database_con = 'mysql+pymysql://root:Golu12#$@localhost:3306/fitbit'
    mydb = sq.create_engine(database_con)
    
    #storing direcct and file name in local
    directory = r'C:\Users\admin\Downloads\ '.strip()   # path of csv file
    csvFileName = 'fitbitdata.csv'

    #Making Csv path for reading
    df = pd.read_csv(os.path.join(directory, csvFileName))
    tablename = csvFileName[:-4] #for removing extsn
    #command for adding table in sql
    df.to_sql(name=tablename, con=mydb, if_exists = 'replace', index=False)
except Exception as e:
    print(str(e))


#     # 3. convert all the dates avaible in dataset to timestamp format in pandas and in sql you to convert it in date format

# In[5]:


df.dtypes


# In[15]:


#oneway to do
#df.ActivityDate = df.ActivityDate.astype('datetime64') 
#antohter way
df.ActivityDate=pd.to_datetime(df.ActivityDate)


# In[7]:


df.dtypes


# In[17]:


df.head()


# In[86]:


#Alter thetable fied into date
import mysql.connector as con
import pandas as pd
try:
    mydb = con.connect(host='localhost', user='root', passwd ='.....')
    cursor = mydb.cursor()
    df2 = pd.read_sql("select *from fitbit.fitbitdata ", mydb)
    print(df2.dtypes)
    chg= """ALTER TABLE fitbit.fitbitdata MODIFY COLUMN ActivityDate DATE;"""
    cursor.execute(chg)
    print(df2.dtypes)
except Exception as e:
    print(e)


# In[2]:


#4 . Find out in this data that how many unique id's we have 
#df["Id"].unique
set(df['Id'])


# In[15]:


#5 . which id is one of the active id that you have in whole dataset 
#df[max(df['VeryActiveMinutes'])]['Id']
df.groupby(['Id'])['VeryActiveMinutes'].sum().idxmax()
#df[df.VeryActiveMinutes == max(df.VeryActiveMinutes)]['Id']


# In[76]:


df.columns


# In[133]:


#6 . how many of them have not logged there activity find out in terms of number of ids 
#df.groupby(df['Id']).count()
#df['LoggedActivitiesDistance'].value_counts()
df.groupby('Id')['LoggedActivitiesDistance'].sum().apply(lambda x:x<1).value_counts()


# In[21]:


#7 . Find out who is the laziest person id that we have in dataset
#df[df.SedentaryMinutes == max(df.SedentaryMinutes)][['Id','SedentaryMinutes']]
df.groupby('Id')['SedentaryMinutes'].sum().idxmax()


# In[42]:


#8 . Explore over an internet that how much calories burn is required for a healthy person and find out how many healthy person we have in our dataset abs

def Burn_calr(no_of_calry):
    if no_of_calry < (2200*7):
        return "Unhealthy"
    elif no_of_calry > (2200*7):
        return "Healthy"

df.groupby('Id')['Calories'].sum().apply(Burn_calr).value_counts()


# In[51]:


#9. how many person are not a regular person with respect to activity try to find out those 
df[df['TotalSteps'] == 0 ]


# In[65]:


#10 . who is the thired most active person in this dataset find out those in pandas and in sql both . 
df.groupby('Id')['VeryActiveMinutes'].sum().sort_values(ascending=False)[2:3]


# In[70]:


#third largest in SQL
import mysql.connector as con

mydb = con.connect(host= 'localhost', user = 'root', passwd = 'Golu12#$')
cursor = mydb.cursor()
db = "use fitbit"
cursor.execute(db)
query = """select * from (select Id, VeryActiveMinutes, dense_rank() over(order by VeryActiveMinutes desc)r from fitbitdata) as a 
where r=3"""
cursor.execute(query)
select_record = cursor.fetchall()
select_ID = select_record[0][0]
Select_VeryActMin = select_record[0][1]
Select_Rank = select_record[0][2]
print('Rank :' ,Select_Rank)


# In[72]:


#11 . who is the 5th most laziest person avilable in dataset find it out 
df.groupby('Id')['VeryActiveMinutes'].sum().sort_values()[4:5]


# In[74]:


#12 . what is a totla acumulative calories burn for a person find out 
df.groupby('Id')['Calories'].sum()


# In[ ]:




