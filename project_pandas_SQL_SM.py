#!/usr/bin/env python
# coding: utf-8

# In[2]:


#load this data in sql and in pandas with a relation in sql 
import pandas as pd
#sheet1 Orders data
order = pd.read_excel('Superstore_USA.xlsx')
#sheet2 Retruns data
returns = pd.read_excel('Superstore_USA.xlsx',sheet_name='Returns')
#sheet3 users data
user = pd.read_excel('Superstore_USA.xlsx',sheet_name='Users')


# In[2]:


#create CSV for sheet1 ='Orders'
order.to_csv("SuperStore_USA_Orders.csv", index= None, header=True)


# In[3]:


#create CSV for sheet2= 'Returns'
returns.to_csv("SuperStore_USA_Returns.csv", index= None, header=True)


# In[4]:


#create CSV for sheet3  ='Users'
user.to_csv("SuperStore_USA_Users.csv", index= None, header=True)


# In[5]:


import sqlalchemy as s
engine = s.create_engine("mysql+pymysql://'root':Golu12#$@localhost:3306/Task")
ord_csv = pd.read_csv("SuperStore_USA_Orders.csv")


# In[7]:


ret_csv = pd.read_csv("SuperStore_USA_Returns.csv")
usr_csv = pd.read_csv("SuperStore_USA_Users.csv")


# In[8]:


#2 . while loading this data you dont have to create a table manually you can use any automated approach to create a table and load a data in bulk in table
#Create sql table for Order ,Returns and USer sheet
ord_csv.to_sql(name='SuperStore_USA_Orders', con=engine, if_exists='replace', index =False)
ret_csv.to_sql(name='SuperStore_USA_Returns', con=engine, if_exists='replace', index= False)
usr_csv.to_sql(name='SuperStore_USA_Users', con=engine, if_exists = 'replace', index=False)


# In[3]:


#3 . Find out how return that we ahve recived and with a product id 
import pandas as pd
import sqlalchemy as s

engine = s.create_engine("m...........")
#reading the ORder table in to dataframe 
or_df= pd.read_sql_table("superstore_usa_orders", engine)
#reading the Retruns table in to dataframe 
rt_df = pd.read_sql_table("superstore_usa_returns", engine)


# In[26]:


#pd.merge(rt_df,or_df, on ='Order ID').count()
pf=pd.merge(rt_df,or_df, on ='Order ID')


# In[36]:


pf[['Order ID', 'Status', 'Product Name']]


# In[39]:


#4 . try  to join order and return data both in sql and pandas 
#join in pandas
T4=pd.merge(rt_df,or_df, on ='Order ID')
T4.head(5)


# In[42]:


#join in sql
import mysql.connector as con
mydb = con.connect(host='localhost', user='root', passwd= '.......', database= 'task')
cursor =mydb.cursor()
join_qury = """SELECT * FROM task.superstore_usa_orders join task.superstore_usa_returns on task.superstore_usa_returns.`Order ID` = task.superstore_usa_orders.`Order ID`"""
cursor.execute(join_qury)
jrcd =cursor.fetchall()

for rcd in jrcd:
    print(rcd)


# In[43]:


or_df.columns


# In[68]:


#5 . Try to find out how many unique customer that we have 
Number_total_Uniq = set(or_df['Customer ID'])
print('unique record thru set: ', len(Number_total_Uniq))
no_urcd = or_df['Customer ID'].nunique()
print("Unique rcord using Nuniue: ", no_urcd)


# In[70]:


#to get the unique reord counts for each row 
or_df.nunique()


# In[2]:


#6 . try to find out in how many regions we are selling a product and who is a manager for a respective region 
or_df= pd.read_sql_table("superstore_usa_orders", engine)
us_df = pd.read_sql_table("superstore_usa_users", engine)
reg = pd.merge(us_df,or_df, on ='Region')
nu_of_reg = reg['Region'].nunique()
names_region =reg[['Manager', 'Region']].drop_duplicates(subset=['Manager', 'Region'])
print('Number of Region we are selling are :', nu_of_reg)
print('Managers with respective their Regions: \n', names_region)


# In[27]:


#7 . find out how many different differnet shipement mode that we have and what is a percentage usablity of 
#all the shipment mode with respect to dataset 
nu_of_shpmode = or_df['Ship Mode'].nunique()
name_shipm = list(or_df['Ship Mode'].unique())
print('Number of shiping mode :', nu_of_shpmode)
print('Shipment name : ', name_shipm)

Count_ech_mode = or_df['Ship Mode'].value_counts()
Total_c_sm = sum(or_df['Ship Mode'].value_counts())
perc = (Count_ech_mode/Total_c_sm) *100
perc


# In[44]:


or_df['Ship Date'] = or_df['Ship Date'].astype('datetime64')
or_df['Order Date'] = pd.to_datetime(or_df['Order Date'])
or_df.dtypes


# In[49]:


#8 . Create a new coulmn and try to find our a diffrence between order date and shipment date
or_df['Duration']=  pd.to_timedelta(or_df['Ship Date']- or_df['Order Date'])
or_df


# In[62]:


#9 . base on question number 8 find out for which order id we have shipment duration more than 10 days
or_df[or_df['Duration'] > pd.to_timedelta(10 ,'D')][['Order ID', 'Ship Date', 'Order Date', 'Duration']]


# In[76]:


#10 . Try to find out a list of a returned order which sihpment duration was more then 15 day
# and find out that region manager as well 
Mer_df = or_df.merge(rt_df, on='Order ID')
#Mer_df[Mer_df['Duration'] > pd.to_timedelta(15, 'D')][['Order ID','Duration']] no records 
Mer_df[Mer_df['Duration'] > pd.to_timedelta(5, 'D')][['Order ID','Duration']]


# In[82]:


#11 . Gorup by region and find out which region is more profitable
or_df.groupby('Region')['Profit'].mean().sort_values(ascending =False)


# In[90]:


#12 . Try to find out overalll in which country we are giving more didscount 
or_df.groupby('State or Province')['Discount'].mean().sort_values(ascending = False).head()


# In[99]:


#13 . Give me a list of unique postal code 
Uniq_pc = list(or_df['Postal Code'].unique())
Uniq_pc


# In[107]:


#14 . which customer segement is more profitalble find it out . 
or_df.groupby('Customer Segment')['Profit'].mean().sort_values(ascending= False).head()


# In[39]:


#15 . try to find out the 10th most loss making product catagory . 
#or_df['Profit'].sort_values().head(10)
or_df.groupby('Product Category').sum()


# In[38]:


#16 . Try to find out 10 top  product with highest margins 
or_df['Product Base Margin'].sort_values(ascending=False).head(10).drop_duplicates()
or_df.sort_values('Product Base Margin', ascending=False)[['Product Base Margin']].drop_duplicates().head(10)


# In[12]:





# In[ ]:




