#!/usr/bin/env python
# coding: utf-8

# In[1]:


from airflow import DAG
from airflow.utils.dates import days_ago


# In[2]:


from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


# In[13]:


from numpy import random
import pandas as pd
import numpy as np
path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DA0101EN-SkillsNetwork/labs/Data%20files/auto.csv'


# In[9]:


default_arguments = {'owner':'Suryn Chia', 'start_date': days_ago(1)}


# In[16]:


with DAG(
    'trial_abc',
    schedule_interval='@daily',
    catchup= False,
    default_args = default_arguments,
) as dag:
    
    bash_task = BashOperator(
        task_id= 'bash_command',
        bash_command='echo $Today',
        env={'Today':'2021-08-22'},
    )
    
    def read_write_file(a=10):
        
        df = pd.read_csv(path, header=None)
        
        headers = ["symboling","normalized-losses","make","fuel-type","aspiration", "num-of-doors","body-style",
         "drive-wheels","engine-location","wheel-base", "length","width","height","curb-weight","engine-type",
         "num-of-cylinders", "engine-size","fuel-system","bore","stroke","compression-ratio","horsepower",
         "peak-rpm","city-mpg","highway-mpg","price"]
        
        df.columns = headers
        
        dir = '/Users/macbook/Desktop/automobile_info.csv'
        
        df.to_csv(dir,index = True)
        
    python_task = PythonOperator(
        task_id='python_function',
        python_callable= read_write_file,
        op_args=[1],
    )
        


# In[15]:


bash_task >> python_task


# In[ ]:




